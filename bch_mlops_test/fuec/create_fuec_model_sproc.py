import configparser
import snowflake.snowpark as snowpark
from snowflake.snowpark import Session
import snowflake.connector
from snowflake.snowpark.functions import sproc

config = configparser.ConfigParser()
config.read('./connections.ini')

connection_parameters = {
    "account": config['SNOWFLAKE']['account'],
    "user": config['SNOWFLAKE']['user'],
    "private_key_file": config['SNOWFLAKE']['private_key_file'],
    "role": config['SNOWFLAKE']['role'],
    "warehouse": config['SNOWFLAKE']['warehouse'],
    "database": config['SNOWFLAKE']['database'],
    "schema": config['SNOWFLAKE']['schema']
}

session = Session.builder.configs(connection_parameters).create() 

@sproc(name="create_fuec_model", is_permanent=True, stage_location="@test_stage", replace=True, packages=["snowflake-snowpark-python"], session=session)
def create_fuec_model(session: snowpark.Session, database: str, schema: str, version_number: str, feature_store_schema: str, warehouse: str) -> str:
    
    create_fuec_model_format_table_cmd = f"""
        create table if not exists {database}.{schema}.FUEC_MODEL_COMMON_FORMAT (
            DAY DATE,
            DAYOFWEEK NUMBER(1,0),
            DAY_OF_WEEK VARCHAR(10),
            MONTH NUMBER(2,0),
            SERVICE_NAME VARCHAR(40),
            SERVICE_CODE VARCHAR(4),
            PATIENTS NUMBER(25,20),
            BEDDEDDAYS NUMBER(5,0),
            STAY_RATE NUMBER(22,20),
            ADMIT_TYPE VARCHAR(30),
            LOCATION VARCHAR(20),
            SITE_DESC VARCHAR(30),
            STAY_KEY VARCHAR(50),
            STAY_KEY_SEQ NUMBER(10,0),
            EXPECTED_LOS NUMBER(24,20),
            UPDATE_DTTM TIMESTAMP_NTZ(3)
        )
    """

    df_fuec_model_format_table = session.sql(create_fuec_model_format_table_cmd)

    fuec_model_cmd = f"""
    INSERT INTO {database}.{schema}.FUEC_MODEL_COMMON_FORMAT

    WITH Model_input_temp2 AS (

    SELECT 
        SERVICE_NAME as model_feature1, 
        beddeddays, 
        date_part(month,ADMIT_DTTM) as model_feature2, 
        case when observed_holiday_ind = 'Y' then 9 else dayofweek(ADMIT_DTTM) end as model_feature3, 
        admit_type as model_feature4, 
        case when location = 'PACU Mandell' then location else 'Occupied Bed' end as model_feature5, service_code as model_feature6, 
        site_desc as model_feature7, 
        count(*) AS patients 
    FROM DATASCIPROD.STAGE.CPM_MODEL_ENCOUNTERS
    WHERE Patient_type = 'Past'
    and admit_type in ('Emergency', 'Urgent')
    and care_class in ('Inpatient', 'Observation', 'Inpatient Psych', 'Surgery Admit', 'Extended Recovery')
    and beddeddays > 0 
    GROUP BY model_feature1, beddeddays, model_feature2, model_feature3, model_feature4, model_feature5, model_feature6, model_feature7

    ),
    -- xx moving this code up in the process so that I don't have to backfill the missing values

    -- model_beddeddays_max AS (
    -- SELECT model_feature1, model_feature2, model_feature3, model_feature4, model_feature5, model_feature6, model_feature7,  max (beddeddays) AS model_beddeddays_max 
    -- FROM Model_input_temp2
    -- GROUP BY model_feature1, model_feature2, model_feature3, model_feature4, model_feature5, model_feature6, model_feature7
    -- ), 

    model_beddeddays_max AS (
           select 
            max_bedded_day as max_beddedday
            , service_code
        from {database}.{feature_store_schema}.los_ex_fv${version_number}
    ),

    -- XX moving this code up in the process so that I don't have to backfill the missing values
    model_beddeddays_scaffold AS (

    SELECT model_feature1, model_feature2, model_feature3, model_feature4, model_feature5, model_feature6, model_feature7, day_key  AS scaffold_beddeddays FROM model_beddeddays_max t
    LEFT JOIN CONS_PROD.EDW.CONS_DATE_DIM D
        ON D.day_key >= 1
        AND D.day_key <= model_beddeddays_max
    ),

    -- this part of the query calculates the number of bedded cases for each stay day, and also calculates the chances that a patient will be here for that night

    fractional_final as( 

    SELECT s.model_feature1, s.scaffold_beddeddays, m.patients, s.model_feature2, s.model_feature3, s.model_feature4, s.model_feature5, s.model_feature6, s.model_feature7, 
    SUM (m.patients) OVER (PARTITION BY s.model_feature1, s.model_feature2, s.model_feature3, s.model_feature4, s.model_feature5, s.model_feature6, s.model_feature7 ORDER BY s.scaffold_beddeddays desc) AS Running_patients,
    SUM (m.patients) OVER (PARTITION BY s.model_feature1, s.model_feature2, s.model_feature3, s.model_feature4, s.model_feature5, s.model_feature6, s.model_feature7) AS total_patients, 
    SUM (m.patients) OVER (PARTITION BY s.model_feature1, s.model_feature2, s.model_feature3, s.model_feature4, s.model_feature5, s.model_feature6, s.model_feature7 ORDER BY s.scaffold_beddeddays desc) / SUM (m.patients) OVER (PARTITION BY s.model_feature1, s.model_feature2, s.model_feature3, s.model_feature4, s.model_feature5, s.model_feature6, s.model_feature7) AS STAY_RATE

    FROM model_beddeddays_scaffold s
    LEFT JOIN Model_input_temp2 m
    ON s.model_feature1 = m.model_feature1
    AND s.model_feature2 = m.model_feature2
    AND s.model_feature3 = m.model_feature3
    AND s.model_feature4 = m.model_feature4
    AND s.model_feature5 = m.model_feature5
    AND s.model_feature6 = m.model_feature6
    AND s.model_feature7 = m.model_feature7
    and s.scaffold_beddeddays = m.beddeddays
    ),

    -- get all service names
    service_names as (
    select service_name, admit_type, case when location = 'PACU Mandell' then location else 'Occupied Bed' end as location, service_code, site_desc
    FROM DATASCIPROD.STAGE.CPM_MODEL_ENCOUNTERS
    WHERE Patient_type = 'Past'
    group by service_name, admit_type, location, service_code, site_desc
    ),

    -- get count of patients by admit date and service
    Emergencies_patients_temp as (
    select 
    sum(case when ADMIT_DATE < '2022-07-01' then 1.14 else 1 end) as patients, 
    date(ADMIT_DATE) admit_date, service_name, admit_type, case when location = 'PACU Mandell' then location else 'Occupied Bed' end as location, service_code, site_desc
    FROM DATASCIPROD.STAGE.CPM_MODEL_ENCOUNTERS
    --from past_patients
    WHERE Patient_type = 'Past'
    and admit_type in ('Emergency', 'Urgent')
    and care_class in ('Inpatient', 'Observation', 'Inpatient Psych', 'Surgery Admit', 'Extended Recovery')
    and beddeddays > 0
    group by date(ADMIT_DATE), service_name, admit_type, location, service_code, site_desc
    order by date(ADMIT_DATE), service_name, admit_type, location, service_code, site_desc),


    -- join count of patients by admit date and service to cons date dim
    Emergencies_patients_temp2 as (
    select day, s.service_name, case when admit_date is null then 0 else patients end as patients, s.admit_type, s.location, s.service_code, s.site_desc, case when hd.date is not null then 9 else dayofweek(day) end as dayofweek
    from CONS_PROD.EDW.CONS_date_dim d 
    cross join service_names s
    left join Emergencies_patients_temp e
    on d.day = e.admit_date
    and s.service_name = e.service_name
    and s.admit_type = e.admit_type
    and s.location = e.location
    and s.service_code = e.service_code
    and s.site_desc = e.site_desc
    left join DATASCIPROD.STAGE.CPM_HOLIDAY_DIM hd
        on d.day = hd.date
    where day >=  CURRENT_DATE - interval '2 years'
    and day < CURRENT_DATE
    order by day, s.service_name
    ), 


    -- get average number of patients by service name, month, day of week
    emergencies_patients as (
    select avg(patients) patients, service_name, date_part(month,day) month, 
    dayofweek,
    admit_type, location, service_code, site_desc
    from Emergencies_patients_temp2
    group by service_name, date_part(month,day), 
    dayofweek, 
    admit_type, location, service_code, site_desc
    ),

    /*
    -- get average/median/percentile number of beddeddays by service, month, day of week
    emergencies_beddeddays as (
    select 
    --round(avg(beddeddays)) as beddeddays, 
    round(percentile_cont(.6) within group (order by beddeddays)) beddeddays,
    service_name, date_part(month,admit_date) month, dayofweek(ADMIT_DATE) dayofweek
        from past_patients
    where admit_type in ('Emergency', 'Urgent')
    and care_class in ('Inpatient', 'Observation') 
    group by service_name, date_part(month,admit_date) , dayofweek(ADMIT_DATE) 
    ),
    */


    -- using cons date dim as scaffold, apply average number of patients and, average/median/percentile beddeddays 
    emergencies_patients_beddeddays_temp as (

    select day, --dayofweek(day) 
    dayofweek,
    day_of_week, date_part(month,d.day) month, ep.service_name, ep.service_code, ep.site_desc,--eb.beddeddays
    ep.patients, scaffold_beddeddays,
    stay_rate, admit_type, location
    from CONS_PROD.EDW.CONS_date_dim d--limit 50
    left join DATASCIPROD.STAGE.CPM_HOLIDAY_DIM hd
        on d.day = hd.date
    left join emergencies_patients ep
    on date_part(month,d.day) = ep.month
    and case when hd.date is not null then 9 else dayofweek(d.day) end = ep.dayofweek
    --and dayofweek(d.day) = ep.dayofweek
    left join fractional_final eb
    on date_part(month,d.day) = eb.model_feature2
    and case when hd.date is not null then 9 else dayofweek(d.day) end = eb.model_feature3
    --and dayofweek(d.day) = eb.model_feature3
    and ep.service_name = eb.model_feature1
    and ep.admit_type = eb.model_feature4
    and ep.location = eb.model_feature5
    and ep.service_code = eb.model_feature6
    and ep.site_desc = eb.model_feature7
    where day >=  CURRENT_DATE
    and day < CURRENT_DATE + interval '3 months'
    and ep.patients > 0
    order by day, date_part(month,d.day), dayofweek(day)--, day
    --limit 100
    --*/
    )--,

    --testing as (
    -- generate a stay key 
    --emergencies_patients_beddeddays as (
    select day, DAYOFWEEK, day_of_week, month, service_name, service_code, patients, scaffold_beddeddays as BEDDEDDAYS, STAY_RATE, admit_type, location, site_desc,
    --select *, 
    --'FUEC'||to_char(dense_rank() over (order by day, service_name, admit_type),'0000000000') as stay_key, 
    dense_rank() over (order by day, service_name, admit_type, location, service_code, site_desc) as stay_key,
    row_number() over (partition by day, service_name, admit_type, location, service_code, site_desc order by beddeddays) as stay_key_seq, 
    sum(stay_rate) over (partition by day, service_name, admit_type, dayofweek, --day_of_week, 
    month, location, service_code, site_desc) as EXPECTED_LOS,
    current_timestamp as update_dttm
    from emergencies_patients_beddeddays_temp
    --order by day
    --),
    --)

    --select sum(stay_rate) from testing
    """

    # Get the Data
    df_fuec_model = session.sql(fuec_model_cmd)
    df_fuec_model = df_fuec_model.collect()
    success = print("FUEC Model Creation Complete!")
    
    return success

# Test w/ .sql worksheet in Snowflake UI
# CALL HAKKODA_DB.DC_SANDBOX.CREATE_FUEC_MODEL('HAKKODA_DB', 'DC_SANDBOX', '179', 'DEV_FEATURE_STORE', 'HAKKODA_WH')