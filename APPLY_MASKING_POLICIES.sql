CREATE OR REPLACE PROCEDURE PUBLIC.APPLY_MASKING_POLICIES(SOURCE_DB STRING, DESTINATION_DB STRING)
returns string
language JavaScript
execute as caller
AS 
$$
//This procedure finds all the Masking policies in one DB
// and executes the script to create those masking policies in another DB,
// This is really only useful in a cloned DB where the tables and policies are all the same

// See UNSET_MASKING_POLICIES for the script that precedes this


//Get list of masks
GetPolicy = `SHOW MASKING POLICIES IN ` + SOURCE_DB + `.PUBLIC;`
snowflake.execute({sqlText: GetPolicy})


GetPolicyReference = `SELECT "schema_name" || '.' || "name" FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));`
PolicyReferenceResult = snowflake.execute({sqlText: GetPolicyReference})


//For each mask, generate and execute a list of ALTER TABLE queries for each column that the mask is applied to
while (PolicyReferenceResult.next()){
    ApplyMaskingPolicyStmt = ` SELECT 'ALTER TABLE IF EXISTS ` + DESTINATION_DB + `.' || REF_SCHEMA_NAME || '.' || REF_ENTITY_NAME || ' MODIFY COLUMN ' || REF_COLUMN_NAME || ' SET MASKING POLICY ` + DESTINATION_DB + `.' || POLICY_SCHEMA || '.' || POLICY_NAME As ApplyPolicy
                               FROM table(information_schema.policy_references(policy_name => '` + SOURCE_DB + `.` + PolicyReferenceResult.getColumnValue(1) + `'))
                               WHERE REF_SCHEMA_NAME IN (SELECT SCHEMA_NAME from `+ DESTINATION_DB +`.INFORMATION_SCHEMA.SCHEMATA);
                             `
    ApplyMaskingPolicyResult = snowflake.execute({sqlText: ApplyMaskingPolicyStmt})
    
   
    
    //execute the ALTER TABLE queries
    while (ApplyMaskingPolicyResult.next()){
        snowflake.execute({sqlText: ApplyMaskingPolicyResult.getColumnValue(1)})                                          
    }
}
$$;
