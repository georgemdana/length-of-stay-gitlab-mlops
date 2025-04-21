CREATE OR REPLACE PROCEDURE HEALTHCARE.RAW_STG.BULK_FHIR_STARTER("SOURCE_DB" VARCHAR(16777216), "SOURCE_SCHEMA" VARCHAR(16777216), "SOURCE_TABLE_NAME" VARCHAR(16777216), "TARGET_DB" VARCHAR(16777216), "TARGET_SCHEMA" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'bulk_fhir_starter'
EXECUTE AS OWNER
AS 'import snowflake.snowpark as snowpark
from snowflake.snowpark.exceptions import SnowparkSQLException
from snowflake.snowpark.functions import col
​
def get_col_type(session, target_db, target_schema, table_name, col_name):
    get_col_type_cmd = f"""
        SELECT DATA_TYPE FROM information_schema.columns 
        WHERE  table_catalog= ''{target_db}''
        AND    table_schema = ''{target_schema}''
        AND    table_name   = ''{table_name}''
        AND    column_name  = upper(''{col_name}'')
    """
    df_col_type = session.sql(get_col_type_cmd).collect()
    return df_col_type
​
def get_variant_array_schema(session, db, schema, source_table_name, variant_arr_col, id_col_nm, target_table_name):
    get_variant_array_types_cmd = f"""
    with array_schema as (
        select 
            regexp_replace(f.path, ''\\\\[[0-9]+\\\\]'', ''[]'') as property
            , typeof(f.value) as col_type
        from (
            select 
                flat_arr.value as {variant_arr_col}
            from {db}.{schema}.{source_table_name},
            lateral flatten(input => {db}.{schema}.{source_table_name}.{variant_arr_col}) AS flat_arr
        ), lateral flatten({variant_arr_col}, recursive => false) f
        group by 1, 2 order by 1, 2
    ),
            
    adjust_col_type as (
        select 
            property
            , case 
                when col_type = ''OBJECT'' then ''VARIANT''
                when col_type = ''VARCHAR'' then ''TEXT''
                else col_type
            end as col_type  
        from array_schema
    )
​
    select
        ''PARENT_RESOURCE'' as property
        ,''TEXT'' as col_type
        ,''PARENT_RESOURCE TEXT'' as resouce_master_col_nm_type
        ,''\\\\\\''{source_table_name}\\\\\\'' as PARENT_RESOURCE'' as resouce_master_col_dict
    union 
    select
        ''SOURCE_COL'' as property
        ,''TEXT'' as col_type
        ,''SOURCE_COL TEXT'' as resouce_master_col_nm_type
        ,''\\\\\\''{variant_arr_col}\\\\\\'' as SOURCE_COL'' as resouce_master_col_dict
    union 
    select
        ''PARENT_ID'' as property
        ,''TEXT'' as col_type
        ,''PARENT_ID TEXT'' as resouce_master_col_nm_type
        ,''{id_col_nm} as PARENT_ID'' as resouce_master_col_dict
    union
    select 
        *
        , concat(property, '' '', col_type ) resouce_master_col_nm_type
        , concat(''{variant_arr_col}:'',property, ''::'',col_type, '' as '', property) resouce_master_col_dict
    from adjust_col_type
    """
    try:
        df_variant_array_types = session.sql(get_variant_array_types_cmd).collect()
​
        target_resouce_schema = {}
        current_resource_create_col = []
        current_resource_insert_col = []
        current_resource_query_col = []
        
        for row in df_variant_array_types:
            current_resource_create_col.append(row.RESOUCE_MASTER_COL_NM_TYPE)
            current_resource_insert_col.append(row.PROPERTY)
            current_resource_query_col.append(row.RESOUCE_MASTER_COL_DICT)
        
        target_resouce_schema[target_table_name] = {
            ''create'': '',''.join(current_resource_create_col),
            ''insert'': '',''.join(current_resource_insert_col),
            ''query'': '',''.join(current_resource_query_col)
        }
​
        return target_resouce_schema
    except SnowparkSQLException as e: 
        print(f''{source_table_name}.{variant_arr_col} raised exception {e}'')
        return []
​
def get_primitive_array_types(session, target_db, target_schema,table_name, primitive_arr_col):
    #TODO update me if used 
    get_primitive_array_types_cmd = f"""
    select regexp_replace(f.path, ''\\\\[[0-9]+\\\\]'', ''{primitive_arr_col}'') as "Path",
    typeof(f.value) as "Type"
    from {target_db}.{target_schema}.{table_name},
    lateral flatten({target_db}.{target_schema}.{table_name}.{primitive_arr_col}, recursive => false) f
    group by 1, 2 order by 1, 2
    """
    try:
        df_variant_array_types = session.sql(get_primitive_array_types_cmd).collect()
        return df_variant_array_types
    except SnowparkSQLException as e: 
        return []
​
​
def get_resouce_schema(session: snowpark.Session, db, schema, table_name):
    # get distinct resources 
    resouce_schema_cmd = f"""
        with resource_schema as (
            select file_row:resourceType::string as fhir_resource
                , regexp_replace(f.path, ''\\\\[[0-9]+\\\\]'', ''[]'') as property
                , typeof(f.value) as col_type
            from {db}.{schema}.{table_name},
            lateral flatten(file_row, recursive => false) f
            group by 1, 2, 3 order by 1, 2, 3 
        ),
        
        adjust_col_type as (
            select 
                fhir_resource
                , property
                , case 
                    when col_type = ''OBJECT'' then ''VARIANT''
                    when col_type = ''VARCHAR'' then ''TEXT''
                    else col_type
                end as col_type  
            from resource_schema
        )
        
        select 
            *
            , concat(property, '' '', col_type ) resouce_master_col_nm_type
            , concat(''file_row:'',property, ''::'',col_type, '' as '', property) resouce_master_col_dict
        from adjust_col_type;
        """
    resouce_schema_df = session.sql(resouce_schema_cmd).collect()
​
    # print(resouce_schema_df)
    
    target_resouce_schema = {}
    current_resource = resouce_schema_df[0].FHIR_RESOURCE
    current_resource_create_col = []
    current_resource_insert_col = []
    current_resource_query_col = []
    
    for row in resouce_schema_df:
        if row.FHIR_RESOURCE != current_resource:
            target_resouce_schema[current_resource] = {
                ''create'': '',''.join(current_resource_create_col),
                ''insert'': '',''.join(current_resource_insert_col),
                ''query'': '',''.join(current_resource_query_col)
            }
            current_resource_create_col = []
            current_resource_insert_col = []
            current_resource_query_col = []
​
        current_resource = row.FHIR_RESOURCE
        current_resource_create_col.append(row.RESOUCE_MASTER_COL_NM_TYPE)
        current_resource_insert_col.append(row.PROPERTY)
        current_resource_query_col.append(row.RESOUCE_MASTER_COL_DICT)
​
    # Last resource 
    target_resouce_schema[current_resource] = {
        ''create'': '',''.join(current_resource_create_col),
        ''insert'': '',''.join(current_resource_insert_col),
        ''query'': '',''.join(current_resource_query_col)
    }
    # print(target_resouce_schema)
​
    return target_resouce_schema
​
def create_resouce_tbl(session, target_db, target_schema, table_name, tbl_col_type ):
        resource_table_create_cmd = f''create table if not exists {target_db}.{target_schema}.{table_name}( {tbl_col_type} )''
        session.sql(resource_table_create_cmd).collect()
​
def insert_resouce_val(session, target_db, target_schema, target_table_name, 
                       source_db, source_schema, source_table_name, 
                       insert_cols, resource, query_columns ):
        resource_table_insert_cmd = f"""insert into {target_db}.{target_schema}.{target_table_name} 
            ({insert_cols})
            select {query_columns} 
            from {source_db}.{source_schema}.{source_table_name} 
            where file_row:resourceType=''{resource}'' """
        df_insert = session.sql(resource_table_insert_cmd).collect()
        return df_insert
​
def insert_target_array_val(session, target_db, target_schema, target_table_name, 
                       source_db, source_schema, source_table_name, 
                       insert_cols, query_columns, variant_arr_col, exec_async ):
        resource_table_insert_cmd = f"""insert into {target_db}.{target_schema}.{target_table_name} 
            ({insert_cols})
            select {query_columns} 
            from (
                select id, flat_arr.value as {variant_arr_col}
                from {source_db}.{source_schema}.{source_table_name},
                lateral flatten(input => {source_db}.{source_schema}.{source_table_name}.{variant_arr_col}) AS flat_arr
            )
            """
        if exec_async:
            return session.sql(resource_table_insert_cmd).collect_nowait()
        else:
            return session.sql(resource_table_insert_cmd).collect()
​
def bulk_fhir_starter(session: snowpark.Session, source_db: str, source_schema: str, source_table_name: str, target_db: str, target_schema: str): 
    target_resouce_schema = get_resouce_schema(session, source_db, source_schema, source_table_name)
    tertiary_mapping = get_tertiary_mapping_v2()
​
    for resource in target_resouce_schema.keys():
        resource_schema = target_resouce_schema[resource]
        resouce_tbl_name = ''FHIR_''+resource.upper()
        print(resource)
        # print(rescource_schema[''create''])
        create_resouce_tbl(session, target_db, target_schema, resouce_tbl_name, resource_schema[''create''] )
        df_insert = insert_resouce_val(session, target_db, target_schema, resouce_tbl_name, 
                       source_db, source_schema, source_table_name, 
                       resource_schema[''insert''], resource, resource_schema[''query''] )
        rows_inserted = df_insert[0][0]
        print(f''{resouce_tbl_name} Rows Inserted:: {rows_inserted}'')
        # if rows insert over zero get mapping and use collect_nowait to process the nodes async 
        if rows_inserted > 0 and resouce_tbl_name in tertiary_mapping:
            print(tertiary_mapping[resouce_tbl_name])
            # print(tertiary_mapping[resouce_tbl_name].keys()) #column names 
            for source_col_name in tertiary_mapping[resouce_tbl_name].keys():
                target_table_name = tertiary_mapping[resouce_tbl_name][source_col_name]
                # print(target_table_name)
                
                #get schema for column 
                df_col_type = get_col_type(session, target_db, target_schema, resouce_tbl_name, source_col_name)
                if df_col_type:
                    if df_col_type[0].DATA_TYPE == ''ARRAY'':
                        variant_array_schema =  get_variant_array_schema(session, target_db, target_schema, resouce_tbl_name, source_col_name, ''id'', target_table_name)
                        target_table_dict =  variant_array_schema[target_table_name]
                        # print(variant_array_schema)
​
                        #create the table if it doesnt exist 
                        create_resouce_tbl(session, target_db, target_schema, target_table_name,target_table_dict[''create''] )
​
                        #Populate the table
                        # need to turn off execute async until adding async controller
                        df_insert = insert_target_array_val(session, target_db, target_schema, target_table_name, 
                            target_db, target_schema, resouce_tbl_name, 
                            target_table_dict[''insert''], target_table_dict[''query''], source_col_name, False )
                        # rows_inserted = df_insert[0][0]
                        # print(f''{target_table_name} Rows Inserted:: {rows_inserted}'')
                        print(f''{target_table_name} Insert initiated'')
                    else:
                        print(''value is not an array'')
                else:
                    print(f''{source_col_name} doesnt exist on table {resouce_tbl_name}'')
              
    return ''FHIR Resources Processed''
​
# Method used in python worksheet     
def main(session: snowpark.Session):
    source_db = ''HEALTHCARE''
    source_schema = ''RAW_STG''
    source_table_name = ''BCA_BULK_FHIR''
    target_db = ''HEALTHCARE''
    target_schema = ''BULK_FHIR''
​
    return bulk_fhir_starter(session, source_db, source_schema, source_table_name, target_db, target_schema)
​
### Trim mapping 
def get_tertiary_mapping_v2():
    tertiary_mapping = {
        "resouce": {
            "property" : ''target fhir table name''
        },
        "FHIR_CAREPLAN": {
            "ACTIVITY" : ''FHIR_CAREPLAN_ACTIVITY''
        },
        "FHIR_CARETEAM": {
            "PARTICIPANT" : ''FHIR_CARETEAM_PARTICIPANT''
        },
        "FHIR_CLAIM": {
            "DIAGNOSIS" : ''FHIR_CLAIM_DIAGNOSIS'',
            "INSURANCE" : ''FHIR_CLAIM_INSURANCE'',
            "ITEM" : ''FHIR_CLAIM_ITEM'',
            "PROCEDURE" : ''FHIR_CLAIM_PROCEDURE''
        },
        "FHIR_DEVICE": {
            "DEVICENAME" : ''FHIR_DEVICE_DEVICE_NAME'',
            "UDICARRIER" : ''FHIR_DEVICE_UDI_CARRIER''
        },
        "FHIR_DIAGNOSTICREPORT": {
            "PRESENTEDFORM" : ''FHIR_ATTACHMENT''
        },
        "FHIR_DOCUMENTREFERENCE": {
            "CONTENT" : ''FHIR_DOCUMENTREFERENCE_CONTENT''
        },
        "FHIR_ENCOUNTER": {
            "PARTICIPANT" : ''FHIR_ENCOUNTER_PARTICIPANT''
        },
        "FHIR_EXPLANATIONOFBENEFIT": {
            "CARETEAM" : ''FHIR_EXPLANATIONOFBENEFIT_CARETEAM'',
            "CONTAINED" : ''FHIR_EXPLANATIONOFBENEFIT_CONTAINED'',
            "DIAGNOSIS" : ''FHIR_EXPLANATIONOFBENEFIT_DIAGNOSIS'',
            "INSURANCE" : ''FHIR_EXPLANATIONOFBENEFIT_INSURANCE'',
            "ITEM" : ''FHIR_EXPLANATIONOFBENEFIT_ITEM''
        },
        "FHIR_MEDICATIONREQUEST": {
            "DOSAGEINSTRUCTION" : ''FHIR_DOSAGE''
        },
        "FHIR_PATIENT": {
            "ADDRESS" : ''FHIR_ADDRESS'',
            "COMMUNICATION": "FHIR_PATIENT_COMMUNICATION",
            "EXTENSION": "FHIR_EXTENSION"
        }
​
    }
    return tertiary_mapping';