import configparser
import snowflake.snowpark as snowpark
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col
from snowflake.ml.feature_store import CreationMode, Entity, FeatureStore, FeatureView 

def create_los_feature(session: snowpark.Session, version_number, feature_store_db, feature_store_schema, default_wh):   
    get_max_los_cmd = f""" 
        SELECT 
            max(BeddedDays) AS max_Bedded_Day
            , min(BeddedDays) AS min_Bedded_Day
            , avg(BeddedDays) AS avg_Bedded_Day
            , SERVICE_CODE 
        FROM DATASCIPROD.STAGE.CPM_MODEL_ENCOUNTERS
        where patient_type = 'Past'
        and beddeddays > 0
        GROUP BY SERVICE_CODE
        """

    df_max_los = session.sql(get_max_los_cmd)

    # perform query populating df and show 10 rows
    #  NOTE: do not leave show commands in the code as the output will get logged in the MLOps pipeline 
    # df_max_los.show() #Debug

    # df_service_code = df_max_los.select(col("SERVICE_CODE"))

    # # populate df and get list for entity 
    # df_service_code_pd = df_service_code.to_pandas()
    # service_code_lst = df_service_code_pd['SERVICE_CODE'].to_list()
    # print(service_code_lst)

    fs = FeatureStore(
        session=session,
        database=feature_store_db,
        name=feature_store_schema,
        default_warehouse=default_wh,
        creation_mode=CreationMode.CREATE_IF_NOT_EXIST,
    )

    sc_entity = Entity(
        name="SERVICE_CODE_ENTITY",
        join_keys=['SERVICE_CODE'],
        desc="Service Codes"
    )
    fs.register_entity(sc_entity)
        
    # fs.list_entities().show() #Debug

    # by not using refresh freq this should generate a view not a dynamic table 
    # EX in the name indicates a view or external feature vs dynamic table 
    external_fv = FeatureView(
        name="LOS_EX_FV",
        entities=[sc_entity],
        feature_df=df_max_los,
        #refresh_freq="None",      # None = Feature Store will never refresh the feature data
        desc="LOS feature view"
    )

    registered_fv: FeatureView = fs.register_feature_view(
        feature_view=external_fv,    
        version= version_number,
        block=False,         # whether function call blocks until initial data is available
        overwrite=False,    # whether to replace existing feature view with same name/version
    )    

    # Return value will appear in the Results tab if ran in a worksheet.
    return df_max_los

def run_feature_creation():
    print("Starting creation of MAX LOS feature")

    config = configparser.ConfigParser()
    config.read('./connections.ini')
    
    warehouse = config['SNOWFLAKE']['warehouse']
    database = config['SNOWFLAKE']['database']

    connection_parameters = {
        "account": config['SNOWFLAKE']['account'],
        "user": config['SNOWFLAKE']['user'],
        "private_key_file": config['SNOWFLAKE']['private_key_file'],
        "role": config['SNOWFLAKE']['role'],
        "warehouse": warehouse,
        "database": database,
        "schema": config['SNOWFLAKE']['schema']
    }
    version_number = config['SNOWFLAKE']['version_number'] 
    feature_store_schema = config['SNOWFLAKE']['feature_store_schema'] 

    session = Session.builder.configs(connection_parameters).create() 
    create_los_feature(session, version_number, database, feature_store_schema, warehouse)
    session.close()  

if __name__ == "__main__":
    run_feature_creation()