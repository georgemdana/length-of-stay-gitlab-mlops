import configparser
import snowflake.connector

config = configparser.ConfigParser()
config.read('./connections.ini')

conn = snowflake.connector.connect(
    account = config['SNOWFLAKE']['account'],
    user = config['SNOWFLAKE']['user'],
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