import configparser
import snowflake.snowpark as snowpark
from snowflake.snowpark import Session
from snowflake.snowpark.functions import sproc


def create_ensemble_model(session: snowpark.Session): 

    @sproc(name="create_ensemble_model", is_permanent=True, stage_location="@model_stage", replace=True, packages=["snowflake-snowpark-python"], session=session)
    def create_ensemble_model(session: snowpark.Session, database: str, schema: str, version_number: str, feature_store_schema: str, model_src: str, model_seq: str) -> str:

        create_emsemble_model_format_table_cmd = f"""
            create table if not exists {database}.{schema}.ENSEMBLE_MODEL_COMMON_FORMAT (
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