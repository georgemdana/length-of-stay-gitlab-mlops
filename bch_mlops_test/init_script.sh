# Specify the interpreter
#!/bin/bash

import snowflake.connector
import os

# Create SSH directory
mkdir -p ~/.ssh

# Write SSH keys to files
echo "$SSH_PRIVATE_KEY" > ~/.ssh/id_rsa
chmod 600 ~/.ssh/id_rsa
echo "$SSH_PUBLIC_KEY" > ~/.ssh/id_rsa.pub
chmod 644 ~/.ssh/id_rsa.pub

# Add Snowflake host to known hosts
ssh-keyscan childrenshospitalofboston-zfa13221.snowflakecomputing.com >> ~/.ssh/known_hosts

# Generate SQL file to set public key
echo "ALTER USER $SNOWFLAKE_USER SET RSA_PUBLIC_KEY = '$SSH_PUBLIC_KEY';" > set_public_key.sql

# Execute SQL command to set public key
snowsql -a $SNOWFLAKE_ACCOUNT -u $SNOWFLAKE_USER -r $SNOWFLAKE_ROLE -w $SNOWFLAKE_WAREHOUSE -d $SNOWFLAKE_DATABASE -s $SNOWFLAKE_SCHEMA -f set_public_key.sql

