#######################################################################################################################################
## What: python file to create the pandas dataframe with the .sql code
## Who: Dana George
## When: July 30, 2024
#######################################################################################################################################
import os
import snowflake
from snowflake.snowpark import Session

sf_raw_data = None

account = os.environ.get("account")
user = os.environ.get("user")
private_key_file_name = os.environ.get("private_key_file_name")
warehouse = os.environ.get("warehouse")
role = os.environ.get("role")
database = os.environ.get("database")
schema = os.environ.get("schema")

def create_snowpark_session(user, account, role, warehouse, private_key_path, private_key_passphrase=None):
    connection_params = {
        "account": account,
        "user": user,
        "role": role,
        "warehouse": warehouse,
        "private_key_file": private_key_path
    }

    session = Session.builder.configs(connection_params).create()
    return session

session = create_snowpark_session(user, account, role, warehouse, private_key_file_name)

# Test the connection by running a simple query
try:
    df = session.sql("SELECT CURRENT_TIMESTAMP").collect()
    print("Connection successful. Current timestamp:", df[0][0])
except Exception as e:
    print("Connection failed:", e)
finally:
    session.close()
