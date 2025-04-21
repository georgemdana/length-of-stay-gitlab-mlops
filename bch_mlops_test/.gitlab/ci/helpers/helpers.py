import os
from snowflake.snowpark import Session

def create_snowpark_session():
    private_key = os.getenv("SNOWFLAKE_PRIVATE_KEY").replace('\\n', '\n')
    config = {
        "account": os.getenv("SNOWFLAKE_ACCOUNT"),
        "user": os.getenv("SNOWFLAKE_USER"),
        "private_key": private_key,
        "role": os.getenv("SNOWFLAKE_ROLE"),
        "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
        "database": os.getenv("SNOWFLAKE_DATABASE"),
        "schema": os.getenv("SNOWFLAKE_SCHEMA")
    }
    session = Session.builder.configs(config).create()
    return session