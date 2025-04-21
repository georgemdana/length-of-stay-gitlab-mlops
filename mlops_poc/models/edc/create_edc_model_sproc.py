import configparser
import snowflake.snowpark as snowpark
from snowflake.snowpark import Session
from snowflake.snowpark.functions import sproc


def create_edc_model(session: snowpark.Session): 

    @sproc(name="create_edc_model", is_permanent=True, stage_location="@model_stage", replace=True, packages=["snowflake-snowpark-python"], session=session)
    def create_edc_model(session: snowpark.Session, database: str, schema: str, version_number: str, feature_store_schema: str, model_src: str, model_seq: str) -> str:
        
        create_edc_model_format_table_cmd = f"""
            create table if not exists {database}.{schema}.EDC_MODEL_COMMON_FORMAT (
                MODEL_SOURCE VARCHAR(10),
                MODEL_SEQUENCE NUMBER(38,0),
                MODEL_TIMESTAMP TIMESTAMP_NTZ(9),
                MODEL_SOURCE_DATA VARCHAR(50),
                MODEL_NAME VARCHAR(50),
                MODEL_OUTPUT VARCHAR(25),
                PATIENT_TYPE VARCHAR(25),
                PATIENT_TIMEFRAME VARCHAR(12),
                PATIENT_STRATA VARCHAR(50),
                STAY_KEY VARCHAR(50),
                STAY_KEY_SEQ NUMBER(10,0),
                NUM_OF_PATIENT NUMBER(25,20),
                STAY_RATE NUMBER(25,20),
                STAY_START_DATE DATE,
                STAY_END_DATE DATE
            )
        """
        df_edc_model_format_table = session.sql(create_edc_model_format_table_cmd)

        create_edc_reclassification_rate_table_cmd = f"""
        create or replace TABLE {database}.{schema}.ED_RECLASSIFICATION_RATE (
            ED_RECLASSIFICATION_RATE NUMBER(21,20)
            )
        """
        df_edc_reclassification_rate_table = session.sql(create_edc_reclassification_rate_table_cmd)

        ed_reclassification_cmd = f"""
        -- STEP 1 --
        -- RETURNS A SINGLE VALUE: THE RATE OF ED RECLASSIFICATION --

        INSERT INTO {database}.{schema}.ED_RECLASSIFICATION_RATE
        with ed_reclassifications as (
            select stay_key,
            admit_date, min(snapshot_date) as min_snapshot_date,
            case when admit_date = min(snapshot_date) -1 then 0 else 1 end as ed_reclassification_I

        from DATASCIPROD.STAGE.CPM_MODEL_CAKE_SNAPSHOT

        where orig_model_name = 'CP(C)'
        and category_desc = 'ED Admit'
        and stay_date_offset = -1
        and admit_date < current_date - 1
        and SNAPSHOT_DATE < current_date
        group by stay_key, admit_date
        having min(snapshot_date) > '2023-04-20' -- 4/10 is the date of the forst cake snpshot
        order by admit_date, stay_key
        )

        select sum(ed_reclassification_I)/count(*) as ED_Reclassification_rate
            from ed_reclassifications
        where min_snapshot_date - admit_date <= 5
            and admit_date >= current_date - interval '1 month'
        """

        edc_model_cmd = f"""
        --STEP 2--
        INSERT INTO {database}.{schema}.EDC_MODEL_COMMON_FORMAT

        WITH

        max_los AS ( 
        select 
            MAX(max_bedded_day) as max_beddedday
        from {database}.{feature_store_schema}.los_ex_fv${version_number}
        ), 

        beddeddays_scaffold AS (
        SELECT day_key AS scaffold_beddeddays, --service_code,
            max_BeddedDay
        FROM CONS_PROD.EDW.cons_date_dim c LEFT JOIN max_los m
            ON c.day_key BETWEEN 1 AND max_BeddedDay
        WHERE c.day_key >= 1
        ),

        beddeddays_scaffold_self_join AS (
        select '' as model_feature1,
            s1.scaffold_beddeddays as stay_day,
            s2.scaffold_beddeddays as model_feature2,
            '' as model_feature3,
            '' as model_feature4,
            '' as model_feature5
        from beddeddays_scaffold s1
        left join beddeddays_scaffold s2
            on s1.scaffold_beddeddays >= s2.scaffold_beddeddays
        ),

        fill_model_temp as (
        SELECT count(*) as Patients,
            '' as model_feature1,
            --SERVICE_CODE as model_feature1,
            BeddedDays,-- as model_feature2,
            '' as model_feature3,
            '' as model_feature4,
            '' as model_feature5
        FROM DATASCIPROD.STAGE.CPM_MODEL_ENCOUNTERS
        where patient_type = 'Past'
            and beddeddays > 0
        GROUP BY --SERVICE_CODE,
            beddeddays
        ),


        fractional_final as(

        SELECT s.model_feature1, stay_day, patients, s.model_feature2, s.model_feature3, s.model_feature4, s.model_feature5,
            SUM (patients) OVER (PARTITION BY s.model_feature1, s.model_feature2, s.model_feature3, s.model_feature4, s.model_feature5 ORDER BY stay_day desc) AS Running_patients,
            SUM (patients) OVER (PARTITION BY s.model_feature1, s.model_feature2, s.model_feature3, s.model_feature4, s.model_feature5) AS total_patients,
            SUM (patients) OVER (PARTITION BY s.model_feature1, s.model_feature2, s.model_feature3, s.model_feature4, s.model_feature5 ORDER BY stay_day desc) / SUM (patients) OVER (PARTITION BY s.model_feature1, s.model_feature2, s.model_feature3, s.model_feature4, s.model_feature5) AS STAY_RATE

        FROM beddeddays_scaffold_self_join s
        left join fill_model_temp f
            on s.model_feature1 = f.model_feature1
            and s.stay_day = f.beddeddays
        ),

        insert_rows as (
        select '{model_src}' as MODEL_SOURCE,
            '{model_seq}' as MODEL_SEQUENCE,
            current_timestamp(2) as MODEL_TIMESTAMP,
            'CONS_PROD' as MODEL_SOURCE_DATA,
            'ED(C)' as MODEL_NAME,
            'Fractional Stay' as MODEL_OUTPUT,
            'Simulated Aggregate' as PATIENT_TYPE,
            patient_type as PATIENT_TIMEFRAME,
            'Current Patients Cardboard' as PATIENT_STRATA,
            CONTACT_SERIAL_NUMBER as STAY_KEY,
            l.stay_day as stay_key_seq,
            ed_reclassification_rate as num_of_patient,
            stay_rate,
            admit_date + l.stay_day - 1 as stay_start_date,
            admit_date + l.stay_day as stay_end_date
            from DATASCIPROD.STAGE.CPM_MODEL_ENCOUNTERS v
            LEFT JOIN fractional_final l
        ON v.beddeddays = l.model_feature2
            cross join DATASCIPROD.STAGE.CPM_MODEL_ED_RECLASSIFICATION_RATE
        --cross join
        where patient_type = 'Current'
            and v.beddeddays > 0
            and care_class = 'Emergency'
        -- getting  only next 3 months of data
            and admit_date + l.stay_day - 1 < current_date + interval '3 months'

        ),

        insert_rows_add_days_temp1 as (
        select distinct model_source, MODEL_SEQUENCE, MODEL_TIMESTAMP, MODEL_SOURCE_DATA, MODEL_NAME, MODEL_OUTPUT, i.PATIENT_TYPE, PATIENT_TIMEFRAME, PATIENT_STRATA, STAY_KEY, num_of_patient, admit_date
        from insert_rows i
        left join DATASCIPROD.STAGE.CPM_MODEL_ENCOUNTERS e
            on i.stay_key = e.CONTACT_SERIAL_NUMBER
        ),

        insert_rows_add_days_temp2 as (
        select model_source, MODEL_SEQUENCE, MODEL_TIMESTAMP, MODEL_SOURCE_DATA, MODEL_NAME, MODEL_OUTPUT, PATIENT_TYPE, PATIENT_TIMEFRAME, PATIENT_STRATA, STAY_KEY,
        row_number() over (partition by stay_key order by d.day) as stay_key_seq, num_of_patient, 1 as stay_rate, d.day as stay_start_date, d.day + 1 as stay_end_date
        from insert_rows_add_days_temp1 i
        left join CONS_PROD.EDW.cons_date_dim d
            on d.day >= i.admit_date
            and d.day < current_date - 1
        where i.admit_date < current_date - 1
        ),

        insert_rows_final as (

        select * from insert_rows_add_days_temp2 --where stay_key = 6136779432
            union all
        select * from insert_rows
        )

        select * from insert_rows_final -- insert_rows
        """

        # Get the Data 
        ed_reclassification = session.sql(ed_reclassification_cmd)
        ed_reclassification = ed_reclassification.collect()

        df_edc_model = session.sql(edc_model_cmd)
        df_edc_model = df_edc_model.collect()
        success = "EDC Model Creation Complete!"
        
        return success

    # Test w/ .sql worksheet in Snowflake UI
    # CALL HAKKODA_DB.DC_SANDBOX.CREATE_EDC_MODEL('HAKKODA_DB', 'DC_SANDBOX', '179', 'DEV_FEATURE_STORE','EDC', '1');

def create_model_stage(session: snowpark.Session, database, schema):
    create_model_stage_cmd = f"create stage if not exists {database}.{schema}.model_stage"
    session.sql(create_model_stage_cmd).collect()
    return 'Stage Created'

def run_model_creation():
    config = configparser.ConfigParser()
    config.read('./connections.ini')

    warehouse = config['SNOWFLAKE']['warehouse']
    database = config['SNOWFLAKE']['database']
    schema = config['SNOWFLAKE']['schema']

    connection_parameters = {
        "account": config['SNOWFLAKE']['account'],
        "user": config['SNOWFLAKE']['user'],
        "private_key_file": config['SNOWFLAKE']['private_key_file'],
        "role": config['SNOWFLAKE']['role'],
        "warehouse": warehouse,
        "database": database,
        "schema": schema
    }

    session = Session.builder.configs(connection_parameters).create()
    create_model_stage(session, database, schema)
    create_edc_model(session)
    session.close()  

if __name__ == "__main__":
    run_model_creation()