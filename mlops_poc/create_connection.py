import os

# print(os.environ)
# CI_COMMIT_BRANCH The commit branch name. Available in branch pipelines, including pipelines for the default branch. Not available in merge request pipelines or tag pipelines.
# CI_COMMIT_REF_NAME The branch or tag name for which project is built.
# CI_PIPELINE_ID The instance-level ID of the current pipeline. This ID is unique across all projects on the GitLab instance.
account = os.environ.get("account")
user = os.environ.get("user")
private_key_file_name = os.environ.get("private_key_file_name")
warehouse = os.environ.get("warehouse")
role = os.environ.get("role")
database = os.environ.get("database")
schema = os.environ.get("schema")
feature_store_schema = os.environ.get("feature_store_schema")
version_number = os.environ.get("CI_PIPELINE_ID")

branch_name = os.environ.get("CI_COMMIT_REF_NAME")
print(f'Branch Name {branch_name}')
if branch_name == 'dev':
    schema = 'DEV'
    feature_store_schema = 'DEV_FEATURE_STORE'
elif branch_name == 'qa':
    schema = 'QA'
    feature_store_schema = 'QA_FEATURE_STORE'
elif branch_name == 'main':
    schema = 'PROD'
    feature_store_schema = 'PROD_FEATURE_STORE'

# create connection file that can be reused across all steps 
# only specify private_key_file_pwd if key is encrypted 
with open("connections.ini", "w") as f:  
    f.write("[SNOWFLAKE]\n")       
    f.write(f'account = {account}\n')  
    f.write(f'user = {user}\n') 
    f.write(f'private_key_file = .secure_files/{private_key_file_name}\n') 
    f.write(f'warehouse = {warehouse}\n') 
    f.write(f'role = {role}\n') 
    f.write(f'database = {database}\n') 
    f.write(f'schema = {schema}\n')
    f.write(f'feature_store_schema = {feature_store_schema}\n') 
    f.write(f'version_number = {version_number}\n')       
    # File closed automatically