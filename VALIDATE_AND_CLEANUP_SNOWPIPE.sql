CREATE OR REPLACE PROCEDURE PUBLIC.VALIDATE_AND_CLEANUP_SNOWPIPE(TABLE_NAME_WITH_SCHEMA VARCHAR, UTC_DATE_TO_VALIDATE VARCHAR)
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS $$
//This procedure compares a table to the storage account that it pulls from, loads any files that are not in the table, and removes duplicates
//This procedure relies on naming standards, and would need to be tweaked if those standards were to change
//e.g. tables/stages/pipes are named <TABLE_NAME>, <TABLE_NAME>_STAGE, <TABLE_NAME>_PIPE

//Caller needs access to see the pipe DDL, execute LIST on the Stage, and edit data in the table

//Need specific date formats to build our standard directory/file name
d = new Date(UTC_DATE_TO_VALIDATE);
year = d.getFullYear();
month = d.getMonth() < 9 ? '0' + (d.getMonth() + 1) : d.getMonth() + 1;
day = d.getDate() < 10 ? '0' + d.getDate() : d.getDate();

var directory = "/" + year + "/" + month + "/" + day;
var file_pattern = ".*" + directory + ".*";
var output = "";

//Get the COPY INTO statement from the snowpipe that is pointed at this table.
var get_copy_statement_sql = "select get_ddl('pipe', '" + TABLE_NAME_WITH_SCHEMA + "_PIPE') as ddl, REPLACE(SUBSTR(DDL, CHARINDEX('COPY INTO', DDL)), ';', '') as COPY_INTO"
rs = snowflake.execute({sqlText: get_copy_statement_sql})
rs.next();
copy_into_statement =  rs.getColumnValue('COPY_INTO');

//Need to modify the COPY INTO script to ensure that it uses two part naming
// First regex gets everything after COPY INTO and before whitespace or a parenthesis and replaces it with the schema/table name
copy_into_statement = copy_into_statement.replace(/(?<=COPY INTO )[^\(\s]+/, TABLE_NAME_WITH_SCHEMA)

// Second regex gets a string that starts with an @ and ends with _STAGE and replaces it with the expected stage syntax with two part naming
copy_into_statement = copy_into_statement.replace(/@\S*_STAGE/, "@" + TABLE_NAME_WITH_SCHEMA + "_STAGE")

snowflake.execute({sqlText: "LIST @" + TABLE_NAME_WITH_SCHEMA + "_STAGE pattern = '" + file_pattern + "'"});

//Get the list of files that are found on Azure, but not in the table for the given date
var get_files_sql = `
    SELECT azurefilename
    FROM
    (
        SELECT s.$1 as azurefilename, t.file_name as tablefilename
        FROM
        (
            table(RESULT_SCAN(LAST_QUERY_ID()))
        ) s
        FULL OUTER JOIN 
        (
            select distinct file_name from ` + TABLE_NAME_WITH_SCHEMA + `
            where file_name like '%' || '` + directory + `' || '%'
        ) t
        ON charindex(t.file_name, s.$1) > 0
    ) a
    WHERE tablefilename IS null`;

files_rs = snowflake.execute({sqlText: get_files_sql});

files_list = [];
while (files_rs.next()){
    //Get the part of the filename that starts with a date (a slash followed by 4 digits in a row)
    //Because the filename prefix is handled by the STAGE
    files_list.push(files_rs.getColumnValue(1).match(/\/\d{4}.*/))
}

//Load files into Snowflake using the pipe COPY INTO statement if there are any
if(files_list.length != 0){
    //Copy files in batches of 1000 because that is the limit for a COPY INTO statement
    for(i = 0; i < (files_list.length / 1000); i++ ){
        files_string = files_list.slice(i*1000, (i+1)*1000).join("', '")

        var load_missing_files_sql = copy_into_statement + `
            files = ('` + files_string + `');`;

        cleanup_rs = snowflake.execute({sqlText: load_missing_files_sql});
    }
  output += "Files Loaded from Azure\n";
}
else{
    output += "Files Match\n";
}

//Soft delete any records that have the duplicate FILENAME, FILE_ROW_NUMBER
var soft_delete = `UPDATE ` + TABLE_NAME_WITH_SCHEMA + ` t
    SET IS_ACTIVE = 0
    FROM (
        select FILENAME, FILE_ROW_NUMBER, ETL_DATETIME
        FROM ` + TABLE_NAME_WITH_SCHEMA + `
        QUALIFY ROW_NUMBER() OVER (PARTITION BY FILENAME, FILE_ROW_NUMBER ORDER BY ETL_DATETIME) > 1
    ) s
    WHERE t.FILENAME = s.FILENAME AND t.FILE_ROW_NUMBER = s.FILE_ROW_NUMBER AND t.ETL_DATETIME = s.ETL_DATETIME;`;

soft_delete_rs = snowflake.execute({sqlText: soft_delete});
soft_delete_rs.next();
output += "Soft Deleted " + soft_delete_rs.getColumnValue(1) + " rows";

return output;
$$;
