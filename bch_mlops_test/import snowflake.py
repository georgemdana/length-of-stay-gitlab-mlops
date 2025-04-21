import snowflake.snowpark as snowpark
from snowflake.ml.model import custom_model
from snowflake.ml.registry import Registry
import pandas as pd

class ExampleTableFunctionModel(custom_model.CustomModel):
    def __init__(self, session, model_context) -> None:
        super().__init__(model_context)
        self.session = session

    @custom_model.inference_api
    def predict(self, input: pd.DataFrame) -> pd.DataFrame:
        query = """
            SELECT 
                'test' as MODEL_SOURCE, 
                'test' as MODEL_SEQUENCE, 
                TO_CHAR(MODEL_TIMESTAMP) as MODEL_TIMESTAMP, 
                MODEL_SOURCE_DATA, 
                MODEL_NAME, 
                MODEL_OUTPUT, 
                PATIENT_TYPE, 
                PATIENT_TIMEFRAME, 
                PATIENT_STRATA, 
                STAY_KEY, 
                STAY_KEY_SEQ, 
                '1' as NUM_OF_PATIENT, 
                '1' as STAY_RATE, 
                TO_CHAR(STAY_START_DATE) as STAY_START_DATE, 
                TO_CHAR(STAY_END_DATE) as STAY_END_DATE 
            FROM CPM_MODEL_COMMON_FORMAT
            """
      
        result = session.sql(query).collect()
        output_df = pd.DataFrame(result)
        return output_df


def main(session: snowpark.Session):

    query = """
        SELECT 
            'test' as MODEL_SOURCE, 
            'test' as MODEL_SEQUENCE, 
            TO_CHAR(MODEL_TIMESTAMP) as MODEL_TIMESTAMP, 
            MODEL_SOURCE_DATA, 
            MODEL_NAME, 
            MODEL_OUTPUT, 
            PATIENT_TYPE, 
            PATIENT_TIMEFRAME, 
            PATIENT_STRATA, 
            STAY_KEY, 
            STAY_KEY_SEQ, 
            '1' as NUM_OF_PATIENT, 
            '1' as STAY_RATE, 
            TO_CHAR(STAY_START_DATE) as STAY_START_DATE, 
            TO_CHAR(STAY_END_DATE) as STAY_END_DATE 
        FROM CPM_MODEL_COMMON_FORMAT
        """
    # Print a sample of the dataframe to standard output.
    result = session.sql(query).collect()

    # Convert the result to a pandas DataFrame
    result_df = pd.DataFrame(result)
    
    my_model = ExampleTableFunctionModel()
    reg = Registry(session, database_name="HAKKODA_DB", schema_name="SANDBOX")
    mv = reg.log_model(my_model,
                model_name="test_cpc_model",
                version_name="vtest",
                options={"function_type": "TABLE_FUNCTION"},
                sample_input_data=result_df.head()
                )
    output_df = mv.run(result_df)

    return output_df