# Snowflake Model Monitor

## Streamlit App Overview:

### 1. Model Dictionary Tab:
- List models stored in your Snowflake Environment
- List important information about those models
    - Name, date created, access, location, description

### 2. Model Accuracy Tab:
- Provides metrics to determine model accuracy
    - Performance, accuracy

### General:
- Utilizes Streamlit for building the user interface.
- Uses Snowpark for executing Snowflake SQL commands.
- Employs helper functions for executing SQL commands, saving environment details, and managing environments.

## Helpers Functions Overview

### 1. `create_snowpark_session(username, password, account, role = "ACCOUNTADMIN", warehouse = "COMPUTE_WH")`:
- **Purpose**: Creates a Snowpark session for executing Snowflake SQL commands.
- **Parameters**: 
  - `username`: Snowflake username.
  - `password`: Snowflake password.
  - `account`: Snowflake account name.
  - `role` (optional, default: "ACCOUNTADMIN"): Snowflake role.
  - `warehouse` (optional, default: "COMPUTE_WH"): Snowflake warehouse.
- **Returns**: Snowpark session object.

### 2. `execute_sql(session, command)`:
- **Purpose**: Executes SQL commands using the provided Snowpark session.
- **Parameters**: 
  - `session`: Snowpark session object.
  - `command`: SQL command to execute.
- **Returns**: Execution result.

### 3. `execute_sql_pandas(session, command)`:
- **Purpose**: Executes SQL commands using the provided Snowpark session and returns the result as a Pandas DataFrame.
- **Parameters**: 
  - `session`: Snowpark session object.
  - `command`: SQL command to execute.
- **Returns**: Execution result as a Pandas DataFrame.

### 4. `querify_list(list)`:
- **Purpose**: Converts a list into a comma-separated string.
- **Parameters**: 
  - `list`: List of items.
- **Returns**: Comma-separated string.

### 5. `save_env(environment_name, fr_name, wh_name)`:
- **Purpose**: Saves environment details such as environment name, functional role (FR) name, and warehouse (WH) name to a YAML file.
- **Parameters**: 
  - `environment_name`: Name of the environment.
  - `fr_name`: Functional role name.
  - `wh_name`: Warehouse name.

### 6. `read_data(environment_name)`:
- **Purpose**: Reads environment details from a YAML file.
- **Parameters**: 
  - `environment_name`: Name of the environment.
- **Returns**: Environment details (database name, FR name, WH name).

### 7. `query_executions(session, queries)`:
- **Purpose**: Executes a list of SQL queries using the provided Snowpark session and generates a DataFrame summarizing the execution status of each query.
- **Parameters**: 
  - `session`: Snowpark session object.
  - `queries`: List of SQL queries to execute.
- **Returns**: DataFrame summarizing the execution status of each query.


