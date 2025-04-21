import configparser
import snowflake.connector
import os

config = configparser.ConfigParser()
config.read('./connections.ini')

private_key_path = config['SNOWFLAKE']['private_key_file']

# Debug information
print(f"Private key path: {private_key_path}")
print(f"Private key file exists: {os.path.exists(private_key_path)}")
print(f"Private key file size: {os.path.getsize(private_key_path)} bytes")
with open(private_key_path, 'r') as key_file:
    print(f"Private key contents:\n{key_file.read()}")

print("Connection parameters:")
for key, value in config['SNOWFLAKE'].items():
    print(f"{key}: {value}")

conn = snowflake.connector.connect(
    account = config['SNOWFLAKE']['account'],
    user = config['SNOWFLAKE']['user'],
    #private_key_path=private_key_path,
    private_key_file = config['SNOWFLAKE']['private_key_file'],
    warehouse = config['SNOWFLAKE']['warehouse'],
    role = config['SNOWFLAKE']['role'],
    database = config['SNOWFLAKE']['database'],
    schema = config['SNOWFLAKE']['schema']
)

cur = conn.cursor()
cur.execute('SELECT current_version()')
print(f'Current Snowflake Version: {cur.fetchone()[0]}')

cur.close()
conn.close()