#########################################################################################################################################
## What: This file will load the data needed to build 1 model, preprocess that data if necessary, train and evaluate and save the model
## Who: Dana George
## When: July 31, 2024
#########################################################################################################################################
import configparser
from snowflake.snowpark import Session
import os
import pandas as pd
import sys

model1df = None

config = configparser.ConfigParser()
config.read('./connections.ini')

account = os.environ.get("account")
user = os.environ.get("user")
private_key_file_name = os.environ.get("private_key_file_name")
warehouse = os.environ.get("warehouse")
role = os.environ.get("role")

def create_snowpark_session(user, private_key_file, account, role, warehouse):

    connection_params = {
    "user" : user,
    "private_key_file" : private_key_file,
    "account" : account,
    "role" : role,
    "warehouse" : warehouse
    }
    # create snowpark session
    session = Session.builder.configs(connection_params).create()
    return session

# def load_data():
#     session = create_snowpark_session(user, private_key_file_name, account, role, warehouse)

#     # Get the Data
#     sql = "SELECT * FROM DATASCIPROD.STAGE.CPM_MODEL_ENCOUNTERS"
#     sf_raw_data = session.sql(sql)

#     # Global Variables
#     def set_raw_dataframe(df):
#         global sf_raw_data
#         sf_raw_data = df

#     def get_raw_dataframe():
#         global sf_raw_data
#         return sf_raw_data

def build_model1():
    session = create_snowpark_session(user, private_key_file_name, account, role, warehouse)

    modle1sql = """
    INSERT INTO DATASCIPROD.STAGE.CPM_MODEL_COMMON_FORMAT


    WITH
    max_los AS ( 
    SELECT max(BeddedDays) AS max_BeddedDay, SERVICE_CODE FROM DATASCIPROD.STAGE.CPM_MODEL_ENCOUNTERS
    where patient_type = 'Past'
    and beddeddays > 0
    GROUP BY SERVICE_CODE
    ), 

    beddeddays_scaffold AS (
    SELECT day_key AS scaffold_beddeddays, service_code, max_BeddedDay
    FROM CONS_PROD.EDW.cons_date_dim c LEFT JOIN max_los m
    ON c.day_key BETWEEN 1 AND max_BeddedDay
    WHERE c.day_key >= 1
    AND service_code IS NOT null
    order by service_code, max_beddedday
    ),  

    beddeddays_scaffold_self_join AS (
    select s1.service_code as model_feature1, 
    s1.scaffold_beddeddays as stay_day,
    s2.scaffold_beddeddays as model_feature2,
    '' as model_feature3,  
    '' as model_feature4,  
    '' as model_feature5 
    from beddeddays_scaffold s1
    left join beddeddays_scaffold s2
    on s1.service_code = s2.service_code
    and s1.scaffold_beddeddays >= s2.scaffold_beddeddays 
    ),

    fill_model_temp as (
    SELECT count(*) as Patients, 
    SERVICE_CODE as model_feature1, 
    BeddedDays,-- as model_feature2,  
    '' as model_feature3,  
    '' as model_feature4,  
    '' as model_feature5  
    FROM DATASCIPROD.STAGE.CPM_MODEL_ENCOUNTERS
    where patient_type = 'Past'
    and beddeddays > 0
    GROUP BY SERVICE_CODE, beddeddays
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
    --and s.model_feature2 = f.model_feature2
    --and s.model_feature3 = f.model_feature3
    --and s.model_feature4 = f.model_feature4
    --and s.model_feature5 = f.model_feature5
    ),

    insert_rows as (
    select null as MODEL_SOURCE, 
        null as MODEL_SEQUENCE, 
        current_timestamp(2) as MODEL_TIMESTAMP,
        'CONS_PROD' as MODEL_SOURCE_DATA,
        'CP(C)' as MODEL_NAME,
        'Fractional Stay' as MODEL_OUTPUT,
        'Simulated Aggregate' as PATIENT_TYPE,
        patient_type as PATIENT_TIMEFRAME,
        'Current Patients Cardboard' as PATIENT_STRATA,
        CONTACT_SERIAL_NUMBER as STAY_KEY,
        l.stay_day as stay_key_seq,
        1 as num_of_patient,
        stay_rate,
        admit_date + l.stay_day - 1 as stay_start_date,
        admit_date + l.stay_day as stay_end_date
        from DATASCIPROD.STAGE.CPM_MODEL_ENCOUNTERS v
        LEFT JOIN fractional_final l
    ON v.service_code = l.model_feature1
    AND v.beddeddays = l.model_feature2
    where patient_type = 'Current'
    and v.beddeddays > 0
    and v.care_class <> 'Emergency'
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
    )


    select * from insert_rows_add_days_temp2 
    union all
    select * from insert_rows
    """

    # Get the Data
    model1df = session.sql(modle1sql)

    # Set Global Variables
    def set_model1df(df):
        global model1df
        model1df = df

    return model1df

def main():
    #"""Main function to orchestrate the model building process."""
    #logger.info("Starting model building process")
    
    model1df = build_model1()
    
    #logger.info("Model building process completed")
    return model1df

if __name__ == "__main__":
    main()