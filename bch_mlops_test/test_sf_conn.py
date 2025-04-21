import os
import configparser
from snowflake.snowpark import Session

def create_connections_ini():
    # Retrieve environment variables
    account = os.environ.get("SNOWFLAKE_ACCOUNT")
    user = os.environ.get("SNOWFLAKE_USER")
    private_key_file_name = os.environ.get("PRIVATE_KEY_FILE_NAME", "snowflake_rsa_key.p8")
    warehouse = os.environ.get("SNOWFLAKE_WAREHOUSE")
    role = os.environ.get("SNOWFLAKE_ROLE")
    database = os.environ.get("SNOWFLAKE_DATABASE")
    schema = os.environ.get("SNOWFLAKE_SCHEMA")

    # Create connections.ini file
    config = configparser.ConfigParser()
    config['SNOWFLAKE'] = {
        'account': account,
        'user': user,
        'private_key_file': f'.secure_files/{private_key_file_name}',
        'warehouse': warehouse,
        'role': role,
        'database': database,
        'schema': schema
    }

    with open("connections.ini", "w") as config_file:
        config.write(config_file)

    print("connections.ini file created successfully.")

def test_snowflake_connection():
    # Read the connections.ini file
    config = configparser.ConfigParser()
    config.read('connections.ini')

    # Get connection parameters
    connection_parameters = dict(config['SNOWFLAKE'])

    try:
        # Create Snowflake session
        session = Session.builder.configs(connection_parameters).create()

        # Test the connection
        result = session.sql("SELECT current_version()").collect()
        print(f"Successfully connected to Snowflake. Version: {result[0][0]}")

        # Close the session
        session.close()
    except Exception as e:
        print(f"Failed to connect to Snowflake: {str(e)}")
        raise

if __name__ == "__main__":
    create_connections_ini()
    test_snowflake_connection()




# import os
# from snowflake.snowpark import Session
# from cryptography.hazmat.backends import default_backend
# from cryptography.hazmat.primitives import serialization

# def test_snowflake_connection():
#     # Get environment variables
#     account = os.environ.get('SNOWFLAKE_ACCOUNT')
#     user = os.environ.get('SNOWFLAKE_USER')
#     warehouse = os.environ.get('SNOWFLAKE_WAREHOUSE')
#     database = os.environ.get('SNOWFLAKE_DATABASE')
#     schema = os.environ.get('SNOWFLAKE_SCHEMA')
#     private_key_file_path = 'snowflake_key.pem'
    
#     if not os.path.exists(private_key_file_path):
#         raise ValueError("Private key file not found")

#     # Read the private key from the file
#     with open(private_key_file_path, "rb") as key_file:
#         p_key = serialization.load_pem_private_key(
#             key_file.read(),
#             password=None,
#             backend=default_backend()
#         )

#     # Convert the private key to bytes
#     pkb = p_key.private_bytes(
#         encoding=serialization.Encoding.DER,
#         format=serialization.PrivateFormat.PKCS8,
#         encryption_algorithm=serialization.NoEncryption()
#     )

#     # Create connection parameters
#     connection_parameters = {
#         "account": account,
#         "user": user,
#         "private_key": pkb,
#         "warehouse": warehouse,
#         "database": database,
#         "schema": schema
#     }

#     # Create a Snowpark session
#     try:
#         session = Session.builder.configs(connection_parameters).create()
        
#         # Test the connection
#         df = session.sql("SELECT current_version()")
#         result = df.collect()
#         print(f"Successfully connected to Snowflake using Snowpark. Version: {result[0][0]}")
        
#         # Close the session
#         session.close()
#     except Exception as e:
#         print(f"Failed to connect to Snowflake: {str(e)}")
#         raise

# if __name__ == "__main__":
#     test_snowflake_connection()

# import os
# import base64
# from snowflake.snowpark import Session
# from cryptography.hazmat.backends import default_backend
# from cryptography.hazmat.primitives import serialization
# from cryptography.hazmat.primitives.asymmetric import rsa
# import socket

# def check_snowflake_connectivity():
#     host = "CHILDRENSHOSPITALOFBOSTON.ZFA13221.snowflakecomputing.com"
#     port = 443
#     try:
#         socket.create_connection((host, port), timeout=10)
#         print(f"Successfully connected to {host}:{port}")
#     except Exception as e:
#         print(f"Failed to connect to {host}:{port}: {e}")

# check_snowflake_connectivity()

# import os
# import snowflake.connector

# def test_snowflake_connection():
#     # Get environment variables
#     account = os.environ.get('SNOWFLAKE_ACCOUNT')
#     user = os.environ.get('SNOWFLAKE_USER')
#     warehouse = os.environ.get('SNOWFLAKE_WAREHOUSE')
#     database = os.environ.get('SNOWFLAKE_DATABASE')
#     schema = os.environ.get('SNOWFLAKE_SCHEMA')
#     private_key_file_path = 'snowflake_key.pem'
    
#     if not private_key_file_path:
#         raise ValueError("Private key file not found")

#     # Read the private key from the file
#     with open(private_key_file_path, "rb") as key_file:
#         p_key = serialization.load_pem_private_key(
#             key_file.read(),
#             password=None,
#             backend=default_backend()
#         )

#     # Create a Snowflake connection
#     try:
#         conn = snowflake.connector.connect(
#             account=account,
#             user=user,
#             private_key=p_key,
#             warehouse=warehouse,
#             database=database,
#             schema=schema
#         )
        
#         # Test the connection
#         cur = conn.cursor()
#         cur.execute("SELECT current_version()")
#         result = cur.fetchone()
#         print(f"Successfully connected to Snowflake. Version: {result[0]}")
        
#         # Close the connection
#         conn.close()
#     except Exception as e:
#         print(f"Failed to connect to Snowflake: {str(e)}")
#         raise

# if __name__ == "__main__":
#     test_snowflake_connection()

# def test_snowflake_connection():
#     # Get environment variables
#     account = os.environ.get('SNOWFLAKE_ACCOUNT')
#     user = os.environ.get('SNOWFLAKE_USER')
#     warehouse = os.environ.get('SNOWFLAKE_WAREHOUSE')
#     database = os.environ.get('SNOWFLAKE_DATABASE')
#     schema = os.environ.get('SNOWFLAKE_SCHEMA')
#     encoded_private_key = os.environ.get('SNOWFLAKE_PRIVATE_KEY')
#     private_key_file_path = os.environ.get('SNOWFLAKE_PRIVATE_KEY_FILE')
    
#     if not encoded_private_key:
#         raise ValueError("SNOWFLAKE_PRIVATE_KEY environment variable not set")

#     # Decode the base64 encoded private key
#     private_key_bytes = base64.b64decode(encoded_private_key)
    
#     # Load the private key
#     p_key = serialization.load_pem_private_key(
#         private_key_bytes,
#         password=None,
#         backend=default_backend()
#     )

#     # Ensure the key is in the correct format
#     if isinstance(p_key, rsa.RSAPrivateKey):
#         pkb = p_key.private_bytes(
#             encoding=serialization.Encoding.DER,
#             format=serialization.PrivateFormat.PKCS8,
#             encryption_algorithm=serialization.NoEncryption()
#         )
#     else:
#         raise ValueError("The provided key is not an RSA Private Key")

#     # Create Snowpark session
#     connection_parameters = {
#         "account": account,
#         "user": user,
#         "private_key": pkb,
#         "warehouse": warehouse,
#         "database": database,
#         "schema": schema,
#     }

#     try:
#         session = Session.builder.configs(connection_parameters).create()
        
#         # Test the connection
#         df = session.sql("SELECT current_version()")
#         result = df.collect()
#         print(f"Successfully connected to Snowflake. Version: {result[0][0]}")
        
#         # Close the session
#         session.close()
#     except Exception as e:
#         print(f"Failed to connect to Snowflake: {str(e)}")
#         raise

# if __name__ == "__main__":
#     test_snowflake_connection()



# # Get environment variables
# account = os.environ.get('SNOWFLAKE_ACCOUNT')
# user = os.environ.get('SNOWFLAKE_USER')
# warehouse = os.environ.get('SNOWFLAKE_WAREHOUSE')
# database = os.environ.get('SNOWFLAKE_DATABASE')
# schema = os.environ.get('SNOWFLAKE_SCHEMA')
# snowflake_private_key = os.environ.get('SNOWFLAKE_PRIVATE_KEY')
# snowflake_role = os.environ.get('SNOWFLAKE_ROLE')

# def create_snowpark_session(user, snowflake_private_key, account, snowflake_role, warehouse):

#     connection_params = {
#     "user" : user,
#     "private_key" : snowflake_private_key,
#     "account" : account,
#     "snowflake_role" : snowflake_role,
#     "warehouse" : warehouse
#     }
#     # create snowpark session
#     session = Session.builder.configs(connection_params).create()

# session = create_snowpark_session(user, snowflake_private_key, account, snowflake_role, warehouse)
# session.sql("select * from DATASCIPROD.STAGE.CPM_MODEL_ENCOUNTERS").collect()

# import os
# import base64
# import snowflake.connector
# from cryptography.hazmat.backends import default_backend
# from cryptography.hazmat.primitives import serialization

# def test_snowflake_connection():
#     # Get the base64 encoded private key from environment variable
#     encoded_private_key = os.environ.get('SNOWFLAKE_PRIVATE_KEY')
    
#     if not encoded_private_key:
#         raise ValueError("SNOWFLAKE_PRIVATE_KEY environment variable not set")

#     try:
#         # Decode the base64 encoded private key
#         private_key_bytes = base64.b64decode(encoded_private_key)

#         # Load the private key
#         p_key = serialization.load_pem_private_key(
#             private_key_bytes,
#             password=None,
#             backend=default_backend()
#         )

#         # Connect to Snowflake
#         conn = snowflake.connector.connect(
#             account=os.environ.get('SNOWFLAKE_ACCOUNT'),
#             user=os.environ.get('SNOWFLAKE_USER'),
#             private_key=p_key,
#             warehouse=os.environ.get('SNOWFLAKE_WAREHOUSE'),
#             database=os.environ.get('SNOWFLAKE_DATABASE'),
#             schema=os.environ.get('SNOWFLAKE_SCHEMA')
#         )

#         # Execute a simple query to test the connection
#         cur = conn.cursor()
#         cur.execute("SELECT current_version()")
#         version = cur.fetchone()[0]
#         print(f"Successfully connected to Snowflake. Version: {version}")

#         # Close the connection
#         conn.close()

#     except Exception as e:
#         print(f"Failed to connect to Snowflake: {str(e)}")
#         raise

# if __name__ == "__main__":
#     test_snowflake_connection()


# import os
# from cryptography.hazmat.backends import default_backend
# from cryptography.hazmat.primitives import serialization
# import snowflake.connector

# # Snowflake connection parameters
# account = os.environ['SNOWFLAKE_ACCOUNT']
# user = os.environ['SNOWFLAKE_USER']
# warehouse = os.environ['SNOWFLAKE_WAREHOUSE']
# database = os.environ['SNOWFLAKE_DATABASE']
# schema = os.environ['SNOWFLAKE_SCHEMA']

# # Load the private key
# with open(os.path.expanduser("~/.ssh/snowflake_key.p8"), "rb") as key_file:
#     p_key = serialization.load_pem_private_key(
#         key_file.read(),
#         password=None,
#         backend=default_backend()
#     )

# # Generate the private key bytes
# pkb = p_key.private_bytes(
#     encoding=serialization.Encoding.DER,
#     format=serialization.PrivateFormat.PKCS8,
#     encryption_algorithm=serialization.NoEncryption()
# )

# # Establish connection
# conn = snowflake.connector.connect(
#     account=account,
#     user=user,
#     private_key=pkb,
#     warehouse=warehouse,
#     database=database,
#     schema=schema
# )

# # Create a cursor object
# cur = conn.cursor()

# # Execute a sample query
# cur.execute("SELECT current_version()")

# # Fetch the result
# result = cur.fetchone()
# print(f"Snowflake version: {result[0]}")

# # Close the cursor and connection
# cur.close()
# conn.close()