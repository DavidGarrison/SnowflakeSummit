CREATE OR REPLACE PROCEDURE PUBLIC.UNSET_MASKING_POLICIES(SOURCE_DB STRING, DESTINATION_DB STRING)
returns string​
language JavaScript​
execute as caller​
AS ​
$$
//If you clone a database that has tables with data masks
// The tables will be masked by the policies from the source database, which is not always ideal
// This procedure systematically loops through all of those masks and removes them 
// So that they can be recreated (or not) in the cloned DB
// Definitely be cautious with removing data masks

// See APPLY_MASKING_POLICIES for the follow-up script that recreates them

//Get list of masks
GetPolicy = `SHOW MASKING POLICIES IN ` + SOURCE_DB + `.PUBLIC;`
snowflake.execute({sqlText: GetPolicy})


GetPolicyReference = `SELECT "schema_name" || '.' || "name" FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));`
PolicyReferenceResult = snowflake.execute({sqlText: GetPolicyReference})


//For each mask, generate and execute a list of ALTER TABLE queries for each column that the mask is applied to
while (PolicyReferenceResult.next()){
    UnsetMaskingPolicyStmt = ` SELECT 'ALTER TABLE IF EXISTS  ` + DESTINATION_DB + `.' || REF_SCHEMA_NAME || '.' || REF_ENTITY_NAME || ' MODIFY COLUMN ' || REF_COLUMN_NAME || ' UNSET MASKING POLICY; ' As UnsetPolicy
                               FROM table(information_schema.policy_references(policy_name => '` + SOURCE_DB + `.` + PolicyReferenceResult.getColumnValue(1) + `'))
                               WHERE REF_SCHEMA_NAME IN (SELECT SCHEMA_NAME from `+ DESTINATION_DB +`.INFORMATION_SCHEMA.SCHEMATA);
                               
                              `
    UnsetMaskingPolicyResult = snowflake.execute({sqlText: UnsetMaskingPolicyStmt})
    
    //execute the ALTER TABLE queries
    while (UnsetMaskingPolicyResult.next()){
        snowflake.execute({sqlText: UnsetMaskingPolicyResult.getColumnValue(1)})
    }
}
$$;
