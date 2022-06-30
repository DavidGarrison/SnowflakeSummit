#!/usr/bin/env python3
import snowflake.connector
import sys
import os, os.path
import errno
import re
import argparse
import pytz
from os import path
from datetime import datetime

# TODO: Add Verbose mode?

# Example Call:
# python .\deploy_snowflake_sql_files.py -db DATA_LAKE_TEST -path "c:/git/database.Snowflake.DATA_LAKE" -item 12345 -user "myEmail@qw-corp.com" -role BIUSERS
parser = argparse.ArgumentParser()
parser.add_argument("-db", "--destination_db", help="Database to be deployed to (e.g. prod = DATA_LAKE)")
parser.add_argument("-path", "--repo_path", help="Full path to the repo being deployed from")
parser.add_argument("-item", "--ado_item_number", help="ADO story or task number named in manifest filename. If no manifest, script will exit.", type=int)
parser.add_argument("-user", "--snowflake_user", help="Snowflake account")
parser.add_argument("-role", "--snowflake_role", help="Snowflake role")
parser.add_argument("-p", "--password", help="Optional. Only used by automated accounts.")
parser.add_argument("-noval", "--skip_destination_validation", help="Optional. Intended to only be used in automation.", action="store_true")
parser.add_argument("-allstop", "--stop_on_all_errors", help="Optional. Breaks execution on any errors raised.", action="store_true")
parser.add_argument("-maxoutlen", "--max_output_string_length", help="Optional. Limits the length of output returned.", default=100000, type=int)
parser.add_argument("-maxrows", "--max_rows_returned", help="Optional. Limits the number of rows returned in a SELECT.", default=1000, type=int)
args = parser.parse_args()
print(args)

destination_database = args.destination_db 
repo_path = args.repo_path 
ado_item_number = str(args.ado_item_number)
manifest_path = repo_path + '\\Scripts\\Manifests\\deployment_' + ado_item_number + '.txt' 
print("Looking for manifest here: " + manifest_path)
if path.exists(manifest_path) == False:
    print("No manifest found for " + ado_item_number + ". Nothing to deploy.")
    exit()
else:
    print("Found manifest.")

tz = pytz.timezone('America/Los_Angeles')

# output returned safety measures
max_output_string_length = args.max_output_string_length
max_rows = args.max_rows_returned

# Validate database destination
if args.skip_destination_validation == False:
    db_validation = input('Are you sure you want to deploy to database ' + destination_database + '? Confirm (y/n): ')
    if db_validation[0].lower() not in ['y']:
        print('Stopping deployment.')
        exit()

# Snowflake connection
if args.password:
    sqlSnowflake = snowflake.connector.connect(
    user=args.snowflake_user,
    role=args.snowflake_role,
    account='ws67899.east-us-2.azure',
    password=args.password,
    warehouse='COMPUTE_WH',
    database=destination_database
    )
else:
    sqlSnowflake = snowflake.connector.connect(
    user=args.snowflake_user,
    role=args.snowflake_role,
    account='ws67899.east-us-2.azure',
    authenticator='externalbrowser',
    warehouse='COMPUTE_WH',
    database=destination_database
   )

sqlSnowflake = sqlSnowflake.cursor()

# Parsing manifest
manifest_file = open(manifest_path, "r")

files_to_deploy = []
for filename in manifest_file:
    if len(filename.replace('\n','')) > 0:
        clean_filename = repo_path + filename.replace('\n','').replace('\\','/')
        files_to_deploy.append(clean_filename)
manifest_file.close()
print(str(len(files_to_deploy)) + ' files to be deployed.')

total_failure_count = 0

# Parsing files listed in manifest
for file_path in files_to_deploy:
    print(file_path + ": Executing")
    with open(file_path, "r") as f:
        stmt_attempt = 0
        stmt_failure = 0
        #Splits on any semicolon that is not followed by '$$' later on in the file.
        #This is needed to avoid splitting at statements within procedures
        stmts = re.split(';(?![\S\s]*\$\$)', f.read())
        # remove empty strings (always present with final semicolon)
        stmts = list(filter(None, stmts)) 
        for s in stmts:
            if s.strip(): # eliminates final newlines being treated as statements
                try:
                    stmt_attempt += 1
                    print()
                    print('Executing:')
                    print(s.lstrip()) # removes leading whitespace from lines between steps
                    print('')
                    startTime = datetime.now(tz)
                    print('Starting at: ' + str(startTime))
                    snowCursor = sqlSnowflake.execute(s)
                    print('Query ID: ' + snowCursor.sfqid)
                    print('')
                    print('Output:')
                    outputStr = str(snowCursor.fetchmany(max_rows))[0:max_output_string_length] # limits by num of rows and total length
                    print(outputStr)
                    endTime = datetime.now(tz)
                    print('Completed at: ' + str(endTime))
                    duration = endTime - startTime
                    print('Duration: ' + str(duration))
                    print('')
                except Exception as e:
                    print('##vso[task.logissue type=error] Error: ', getattr(e, 'message', repr(e))) 
                    print('')
                    stmt_failure += 1
                    total_failure_count += 1
                    if args.stop_on_all_errors:
                        raise RuntimeError("Errors in Snowflake Deployment")
                        exit()
        f.close()
        print(f"Statements run: {stmt_attempt} Statements failed: {stmt_failure}")

# Cleanup
sqlSnowflake.close()

if total_failure_count > 0:
    raise RuntimeError(f"Errors in Snowflake Deployment: {total_failure_count}")