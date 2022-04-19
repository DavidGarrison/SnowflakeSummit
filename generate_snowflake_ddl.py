#!/usr/bin/env python
import snowflake.connector
import sys
import os, os.path
import errno
import re
import argparse

#This mask is slightly out of date with how Data masks work in DDLs now.
# And naming conventions may need to be adjusted
# Still useful for one-off uses, but updates are needed for this to be used repeatably

# Example Call:
# generate_snowflake_ddl.py --source_db DATA_LAKE_TEST --repo_path "C:/git/Database.Snowflake.DATA_LAKE" --user dgarrison@qw-corp.com
parser = argparse.ArgumentParser()
parser.add_argument("--source_db", help="Database to be sourced from (defaults to DATA_LAKE_TEST)", default="DATA_LAKE_TEST")
parser.add_argument("--schema", help="Optional. Limits scan to only the specified schema.")
parser.add_argument("--object", help="Optional. Limits scan to only the specified object name. Allows SQL wildcards. Not case sensitive. Do not include two/three part names.")
parser.add_argument("--repo_path", help="Full path to the repo being generated to (defaults to Database.Snowflake.DATA_LAKE)", default="C:/git/Database.Snowflake.DATA_LAKE")
parser.add_argument("--user", help="Snowflake account")
args = parser.parse_args()
print(args)

database = args.source_db 
single_schema = args.schema
object_name = args.object
repo_path = args.repo_path 
sf_user = args.user

sqlSnowflake = snowflake.connector.connect(
user=sf_user,
account='ws67899.east-us-2.azure',
authenticator='externalbrowser',
warehouse='COMPUTE_WH',
database=database
)

sqlSnowflake = sqlSnowflake.cursor()

def mkdir(path):
    try:
        os.makedirs(path)
    except OSError as exc: # Python >2.5
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else: raise

def mkdir_object_type(schema, object_type):
    root = repo_path + '/' + schema + '/'
    #print(root + object_type)
    mkdir(root + object_type)
    
def add_objects(sql, object_type, objects):
    sqlSnowflake.execute(sql)
    resultset = sqlSnowflake.fetchall()
    #print(resultset)

    for result in resultset:
        objects.append((object_type, result[0], result[1]))

schemas = []
if single_schema is not None:
    schemas.append(single_schema)
else:
    sql_schemas = "select SCHEMA_NAME from information_schema.schemata where schema_name not in ('PUBLIC', 'INFORMATION_SCHEMA');"
    sqlSnowflake.execute(sql_schemas)
    schemas = [s[0] for s in sqlSnowflake.fetchall()]

#Prepare Masking Policy queries.
# This is only relevant to tables that have masking policies, but it's generally more efficient to build out the list separately
# Note that this will run even if there are no objects being built that have masking policies applied
# This relies on the convention that Masking policies are only in the PUBLIC schema.
get_masks_sql_ddl = "SHOW MASKING POLICIES IN PUBLIC;"

sqlSnowflake.execute(get_masks_sql_ddl)
snowflake_response = sqlSnowflake.fetchall()
mask_list = [x[3] + "." + x[1] for x in snowflake_response]

mask_references = []
for mask in mask_list:
    get_mask_references = f"SELECT REF_SCHEMA_NAME, REF_ENTITY_NAME, REF_COLUMN_NAME, POLICY_SCHEMA, POLICY_NAME FROM table(information_schema.policy_references(policy_name => '{mask}'));"
    sqlSnowflake.execute(get_mask_references)
    snowflake_response = sqlSnowflake.fetchall()
    mask_references += [[x[0] + "." + x[1], f"ALTER TABLE {x[0]}.{x[1]} MODIFY COLUMN {x[2]} SET MASKING POLICY {x[3]}.{x[4]}"] for x in snowflake_response]

for schema in schemas:
    objects = []

    sql_tables = "SELECT table_name, table_schema || '.\"' || table_name || '\"' table_name FROM INFORMATION_SCHEMA.TABLES WHERE table_schema = '" + schema +"' AND TABLE_TYPE = 'BASE TABLE'"
    if object_name:
        sql_tables += "and table_name ilike '" + object_name + "'"

    add_objects(sql_tables, 'TABLE', objects)
    
    sql_views = "SELECT table_name, table_schema || '.' || table_name table_name FROM INFORMATION_SCHEMA.TABLES WHERE table_schema = '" + schema +"' AND TABLE_TYPE = 'VIEW'"
    if object_name:
        sql_views += "and table_name ilike '" + object_name + "'"

    add_objects(sql_views, 'VIEW', objects)
    
    sql_procedures = "SELECT procedure_name, procedure_schema || '.' || procedure_name || regexp_replace(argument_signature, '(\\\\(|, )\\\\S* ','\\\\1')  procedure_name FROM INFORMATION_SCHEMA.PROCEDURES WHERE procedure_schema = '" + schema + "'"
    if object_name:
        sql_procedures += "and procedure_name ilike '" + object_name + "'"

    add_objects(sql_procedures, 'PROCEDURE', objects)

    sql_pipes = "SELECT pipe_name, pipe_schema || '.' || pipe_name pipe_name FROM INFORMATION_SCHEMA.PIPES WHERE pipe_schema = '" + schema + "'"
    if object_name:
        sql_pipes += "and pipe_name ilike '" + object_name + "'"

    add_objects(sql_pipes, 'PIPE', objects)
  
    #print(objects)

    for obj in objects:
        #print(database, schema, obj[0])
        
        object_type = obj[0]
        short_object_name = obj[1]
        full_object_name = obj[2]
        
        mkdir_object_type(schema, object_type)
        
        get_sql_ddl = f"SELECT GET_DDL('{object_type}', '{full_object_name}');"

        #print (sql_ddl_object)
        
        sqlSnowflake.execute(get_sql_ddl)
        snowflake_response = sqlSnowflake.fetchall()
        sql_ddl = snowflake_response[0][0]
        
        ######################################
        #Cleanup
        ######################################
        sql_ddl = re.sub('\t', '    ', sql_ddl)
        
        #Views can include three part naming. reduce it to just an object name to keep it in line with other objects
        #also they start with a newline for some reason. remove this too.
        if obj[0] == 'VIEW':
            sql_ddl = re.sub(f'CREATE OR REPLACE VIEW ({database}\.)*({schema}\.)*', 'CREATE OR REPLACE VIEW ', sql_ddl, flags=re.IGNORECASE)
            sql_ddl = sql_ddl[1:]
            
            #Add Reader grants to all views that do not have PII
            # 2021-01-01: Not all schemas have been set up with the same PII naming conventions, so double check this in the git compare
            if '_PII_' not in sql_ddl and schema != 'CELLO' and schema != 'DATA_STAGING' :
                sql_ddl += F'\nGRANT SELECT ON VIEW {full_object_name} TO ROLE READER;\n'
            
        #Add the schema name to all DDLs
        sql_ddl = re.sub(f'create or replace {object_type} ', f'CREATE OR REPLACE {object_type} {schema}.', sql_ddl, flags=re.IGNORECASE)
        
        #Cleanup newline formatting issues
        sql_ddl = re.sub('\r\n|\r|\n', '\n', sql_ddl)

        #Change VARCHAR MAX to STRING
        sql_ddl = sql_ddl.replace('VARCHAR(16777216)','STRING')
        
        #Tables shouldn't include "OR REPLACE"
        #Tables need data masks queries added to the DDL
        if obj[0] == 'TABLE':
            sql_ddl = sql_ddl.replace('CREATE OR REPLACE ', 'CREATE ')
            sql_ddl += '\n'

            for reference in [x[1] for x in mask_references if x[0] == full_object_name]:
                sql_ddl += reference + ";\n"

        #Procedures
        if obj[0] == 'PROCEDURE':
            #remove quotes around the object name which are included by default
            sql_ddl = re.sub('"(.*?)"', r'\1', sql_ddl, 1)
            #Replace the outer single quotes with $$ to improve code readability
            #aslo requires replacing '' with ' throughout the code
            sql_ddl = re.sub("AS '", "AS $$", sql_ddl, 1, flags=re.IGNORECASE)
            sql_ddl = re.sub("''", "'", sql_ddl)
            sql_ddl = re.sub("';(?:(?![\S\s]*';))", "$$;", sql_ddl)
            sql_ddl += '\n'
            
        if len(sql_ddl) != 0:
            #print(ddl_result[0][0])
            print(f'{repo_path}/{schema}/{object_type}/{short_object_name}.sql')
            file = open(f'{repo_path}/{schema}/{object_type}/{short_object_name}.sql','w')
            
            file.write(str(sql_ddl))
            file.close()
