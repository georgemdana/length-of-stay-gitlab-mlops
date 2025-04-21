import configparser
import snowflake.snowpark as snowpark
from snowflake.snowpark import Session

def create_root_task(session: snowpark.Session, database, schema, warehouse):
    create_root_task_cmd = f"""
        CREATE OR REPLACE TASK {database}.{schema}.CAKE_ROOT_TASK
            WAREHOUSE = {warehouse}
            SCHEDULE = 'USING CRON 0 4 * * * UTC'
            AS
                SELECT 'Root Task Starting' as msg
        """
    df_create_root_task = session.sql(create_root_task_cmd).collect()

def create_cpc_task(session: snowpark.Session, database, schema, warehouse, version_number, feature_store_schema):
    create_cpc_task_cmd = f"""
        CREATE OR REPLACE TASK {database}.{schema}.CAKE_CPC_TASK
            WAREHOUSE = {warehouse}
            AFTER {database}.{schema}.CAKE_ROOT_TASK
            AS
                CALL {database}.{schema}.CREATE_CPC_MODEL('{database}', '{schema}', '{version_number}', '{feature_store_schema}', 'CPC', '1')
    """
    df_create_cpc_task = session.sql(create_cpc_task_cmd).collect()

def create_fuec_task(session: snowpark.Session, database, schema, warehouse):
    create_fuec_task_cmd = f"""
        CREATE OR REPLACE TASK {database}.{schema}.CAKE_FUEC_TASK
            WAREHOUSE = {warehouse}
            AFTER {database}.{schema}.CAKE_ROOT_TASK
            AS
                CALL {database}.{schema}.CREATE_FUEC_MODEL('{database}', '{schema}')
        """
    df_create_fuec_task = session.sql(create_fuec_task_cmd).collect()

def create_edc_task(session: snowpark.Session, database, schema, warehouse, version_number, feature_store_schema):
    create_fuec_task_cmd = f"""
        CREATE OR REPLACE TASK {database}.{schema}.CAKE_EDC_TASK
            WAREHOUSE = {warehouse}
            AFTER {database}.{schema}.CAKE_ROOT_TASK
            AS
                CALL {database}.{schema}.CREATE_EDC_MODEL('{database}', '{schema}', '{version_number}', '{feature_store_schema}', 'EDC', '1')
        """
    df_create_edc_task = session.sql(create_fuec_task_cmd).collect()

def create_ensemble_task(session: snowpark.Session, database, schema, warehouse):
    create_ensemble_task_cmd = f"""
        CREATE OR REPLACE TASK {database}.{schema}.CAKE_ENSEMBLE_TASK
            WAREHOUSE = {warehouse}
            AFTER {database}.{schema}.CAKE_CPC_TASK, {database}.{schema}.CAKE_FUEC_TASK, {database}.{schema}.CAKE_EDC_TASK
            AS
                -- This is just an example while the ensemble dev is being completed
                create or replace table {database}.{schema}.EXAMPLE_ENSEMBLE
                as
                select * from (select * from {database}.{schema}.CPC_MODEL_COMMON_FORMAT limit 10)
                union
                select * from (select * from {database}.{schema}.EDC_MODEL_COMMON_FORMAT limit 10)
        """
    df_create_ensemble_task = session.sql(create_ensemble_task_cmd).collect()

def resume_tasks(session: snowpark.Session, database, schema):
    resume_tasks_cmd = f"SELECT SYSTEM$TASK_DEPENDENTS_ENABLE('{database}.{schema}.CAKE_ROOT_TASK')"
    df_resume_tasks =session.sql(resume_tasks_cmd).collect()

def run_task_creation():
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

    version_number = config['SNOWFLAKE']['version_number'] 
    feature_store_schema = config['SNOWFLAKE']['feature_store_schema'] 

    session = Session.builder.configs(connection_parameters).create()
    create_root_task(session, database, schema, warehouse)
    create_cpc_task(session, database, schema, warehouse, version_number, feature_store_schema)
    create_fuec_task(session, database, schema, warehouse)
    create_edc_task(session, database, schema, warehouse, version_number, feature_store_schema)

    create_ensemble_task(session, database, schema, warehouse)

    # resume_tasks(session, database, schema) turned off right now until permissions are updated 
    session.close()  

if __name__ == "__main__":
    run_task_creation()