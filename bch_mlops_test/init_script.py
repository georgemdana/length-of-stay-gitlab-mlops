import os
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
import snowflake.connector

# Snowflake connection parameters
account = os.environ['SNOWFLAKE_ACCOUNT']
user = os.environ['SNOWFLAKE_USER']
warehouse = os.environ['SNOWFLAKE_WAREHOUSE']
database = os.environ['SNOWFLAKE_DATABASE']
schema = os.environ['SNOWFLAKE_SCHEMA']

# Load the private key
with open(os.path.expanduser("~/.ssh/snowflake_key.p8"), "rb") as key_file:
    p_key = serialization.load_pem_private_key(
        key_file.read(),
        password=None,
        backend=default_backend()
    )

# Generate the private key bytes
pkb = p_key.private_bytes(
    encoding=serialization.Encoding.DER,
    format=serialization.PrivateFormat.PKCS8,
    encryption_algorithm=serialization.NoEncryption()
)

# Establish connection
conn = snowflake.connector.connect(
    account=account,
    user=user,
    private_key=pkb,
    warehouse=warehouse,
    database=database,
    schema=schema
)

# Create a cursor object
cur = conn.cursor()

# Execute your Snowflake operations here
cur.execute("SELECT current_version()")
result = cur.fetchone()
print(f"Snowflake version: {result[0]}")

# Close the cursor and connection
cur.close()
conn.close()