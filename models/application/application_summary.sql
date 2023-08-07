create or replace table credit_secured.application.application_summary as
SELECT
COALESCE(a.owner_id, b.id) as user_id,
a.application_id as application_id,
current_timestamp as last_update_ts,
CONVERT_TIMEZONE('America/Los_Angeles',_CT) AS application_start_ts,
CONVERT_TIMEZONE('America/Los_Angeles', b.createdAt) as profile_creation_ts,
b.onboardingstatus as current_onboarding_status,
COALESCE(CONVERT_TIMEZONE('America/Los_Angeles',a.STATUS_UPDATED_AT),CONVERT_TIMEZONE('America/Los_Angeles',b.updatedat)) AS last_status_update_ts,
ROW_NUMBER () OVER (PARTITION BY user_id ORDER BY last_status_update_ts DESC) AS application_recency,
--Secured Info
b.givenName::string as first_name,
b.familyName::string as last_name,
b.email::string as email_address,
b.phone::string as phone_number,
b.mailingStreet1::string as address_line_1,
b.mailingStreet2::string as address_line_2,
b.mailingCity::string as address_city,
b.mailingRegion::string as address_state,
b.mailingPostalCode::string as address_zip,
b.mailingCountryCode::string as address_country,
b.dob::date as date_of_birth,
DATEDIFF(years,date_of_birth,to_date(application_start_ts)) as age,
--Unsecured Info
'Mobile' AS application_source,
CASE 
    WHEN USER_ID IN ('ce3cda02-5c63-4caa-a16d-068c7f3b9fc1','9acde48c-f06b-495d-8531-15b672f5a1fc','bcf80dec-5e09-463e-b90b-797209561f0a','74e4e738-4786-4ee1-a19b-42f0c73a7082','79904310-5158-430b-abdd-292d3fe3b385','e8a36bcd-4373-49f7-b70f-6c6a311f154d','fce25b99-c0d1-4427-9efa-0e04e0eddeba','f0699438-31b6-4942-9e84-cdfe29b4157c','49babc6b-18ae-45d9-bb1d-65543b916cbe','6f2e2f33-5f8b-469e-9a45-ae85f98198c5','aed4d33b-e082-4c8a-83c9-ff6a0a2faa93','e0373c7f-e8c4-441e-84d8-c1ce4802bc42','b3547996-b1d3-49d4-ac4a-bfff63100324','5dfd9602-30ad-4dd4-8994-8aedf4def179','8a928404-528d-445d-a371-62a9c7ffaf2d','b4ca0f90-9016-4dfd-b183-1975de7f7b7a','f4fd110b-6b49-45f5-84d8-1cbf6615f733','3fcb09b7-f3de-4ac5-8b07-d5d133b788a2','1fbd1e1b-ac61-4b93-8167-d0490dc35663') THEN 'Alpha'
    WHEN USER_ID IN ('ea8c29ab-9de8-48bd-ba17-4f7b187a10e7','7d3bdb8f-63e1-459d-89db-8936b8e45ee1','7bd28a56-b4bf-414a-988f-0cb318591a59','0813f141-ba10-4827-a74a-4dec72193a6f','e1a927df-1be9-457b-9b01-d1f57dc8d239','89e77c4d-2922-438f-97b0-e7ba1af7d6ad','05f2a385-85ec-470d-9188-cd8ae7685640','a1f591ea-acdc-4c05-bd49-8755cf9ea916','3547bb94-6f19-4d42-b2db-ddfb29f1e35e','fa6975e1-4484-4728-adfe-a45d1c5b637d','8c14f93e-902c-4988-9b97-ffa9421212af','afa75382-e22d-4427-8689-33d546c183ff','37b7d3bd-519b-46d6-ae6c-d0ddad771732','f38197fa-f9b0-4657-a69a-32bb2c1adfde','6242b088-c772-4a48-8394-4ae736e3f93e','375865e1-5ed3-4fb6-af08-376971d5271f','461ed6d2-cc05-4ff8-a3d7-24346eeeecc6','82d6ca94-310d-4753-bcb9-9b367c3b6d41','16dd9a60-9b50-41a8-87b1-68727bf339a5','10e0546e-be22-4686-bcbf-57ddd4f3ec69','50a2192c-1135-40c3-9bdd-9a728b62142c','36cb00b9-d860-4de8-96a1-2d826eaae05c','2a598480-1f43-46a1-9f21-2a1bdefe9cb8','bd07aa00-d234-4b91-b20b-6875d0a1759e','94fd0bcd-86ac-4478-8100-a8a02bc8627c','eade2a84-96e9-4d36-a166-b87412e975f4','efa71065-d49a-443b-a171-7f9661ce681a','0fd6530e-65fb-4006-b1d7-3b27d96c5566') THEN 'Beta'
    WHEN USER_ID IN ('1217ea54-d71f-4cae-bb04-f483d6ab0a7a','9863bdeb-a083-40db-af39-bde65f973672','d9c3eb21-c630-4b4f-925d-2ab0bc32af34','162b623b-75d6-47c0-80bb-d573b89089fb','770bc8ce-61fa-4f2e-a275-652badcfbe44','74b9a553-3b31-4ba2-9f1e-71530ede4557','36823cfb-ae29-4cb8-ad8a-573bae5be057','2e1715e1-ef66-4f3a-91fd-97429c2a31a7','c01269e3-4948-486a-b19b-351336b44179','4ef87d78-6eb9-4d54-9668-6cd84e4a07be','38f15f50-806a-415e-a30a-6f3d89568d70','8bcfe448-f606-4ca7-82ed-3cf31096c388','b974be94-7e52-4616-bac9-36e9dfea5e27','cb4068cf-4ea2-4bcf-ad4e-0d3022825caa','08f24240-8d8c-4ca0-add2-767565924daa','1ca61d60-0fff-48e4-b3b0-ab97f4f66fe5','e2b954d8-72f3-42e4-b42a-bfd903cfc2ef','6c2c9c70-d083-47a9-8a46-8e5a0559e412','40586f92-7467-4e0a-899d-35e78b3aa045','b201151a-b507-4343-a282-a2343ce14f5e') Then 'Confirmed Fraud'
    ELSE 'Rollout'
END AS testing_stage,
rules_versions:credit_policy_version::string as credit_policy_version,
profile:waitList::string as waitlist_ind,
CASE WHEN b.onboardingstatus = 'ManualDecline' then 'failed' ELSE status end as application_status,
CAST(NULL AS STRING) AS detailed_application_status,
CAST(NULL AS STRING) as reject_reason,
socure:reference_id::string as socure_reference_id,
rules_versions:socure_rules_version::string as kyc_rules_version,
CASE WHEN socure:rule_set_passed = 'true' THEN 'Approve' WHEN socure:rule_set_passed = 'false' THEN 'Reject' END AS kyc_decision,
CAST(NULL AS STRING) as kyc_reject_reason,
CAST(NULL AS CHAR(1)) AS docv_ind,
socure:socure_address_risk_reason_codes::string as address_risk_reason_codes,
socure:socure_address_risk_score::decimal(4,3) as address_risk_score,
socure:socure_alert_risk_reason_codes::string as alert_risk_reason_codes,
socure:socure_email_risk_reason_codes::string as email_risk_reason_codes,
socure:socure_email_risk_score::decimal(4,3) as email_risk_score,
socure:socure_global_watchlist_reason_codes::string as global_watchlist_reason_codes,
socure:socure_kyc_field_validation_first_name::decimal(3,2) as kyc_field_validation_first_name,
socure:socure_kyc_field_validation_sur_name::decimal(3,2) as kyc_field_validation_surname,
socure:socure_kyc_field_validation_mobile_number::decimal(3,2) as kyc_field_validation_mobile_number,
socure:socure_kyc_field_validation_dob::decimal(3,2) as kyc_field_validation_dob,
socure:socure_kyc_field_validation_ssn::decimal(3,2) as kyc_field_validation_ssn,
socure:socure_kyc_field_validation_street_address::decimal(3,2) as kyc_field_validation_street_address,
socure:socure_kyc_field_validation_city::decimal(3,2) as kyc_field_validation_city,
socure:socure_kyc_field_validation_state::decimal(3,2) as kyc_field_validation_state,
socure:socure_kyc_field_validation_zip::decimal(3,2) as kyc_field_validation_zip,
socure:socure_kyc_reason_codes::string as kyc_reason_codes,
socure:socure_phone_risk_reason_codes::string as phone_risk_reason_codes,
socure:socure_phone_risk_score::decimal(4,3) as phone_risk_score,
socure:socure_sigma_identity_fraud_reason_codes::string as identity_fraud_reason_codes,
socure:socure_sigma_identity_fraud_score::decimal(4,3) as identity_fraud_score,
socure:socure_sigma_synthetic_fraud_reason_codes::string as synthetic_fraud_reason_codes,
socure:socure_sigma_synthetic_fraud_score::decimal(4,3) as synthetic_fraud_score,
equifax:reference_id::string as equifax_reference_id,
rules_versions:equifax_rules_version::string as credit_rules_version,
equifax_hit_code,
CASE 
    WHEN equifax_hit_code = '1' THEN '1 - Hit'
    WHEN equifax_hit_code = '2' THEN '2 - No-Hit'
    WHEN equifax_hit_code = '3' THEN '3 - Manual File'
    WHEN equifax_hit_code = '4' THEN '4 - Manual File Review Required'
    WHEN equifax_hit_code = '5' THEN '5 - Referred File'
    WHEN equifax_hit_code = '6' THEN '6 - Hit with Automated Consumer Narrative'
    WHEN equifax_hit_code = '7' THEN '7 - Fraud/Verification Product'
    WHEN equifax_hit_code = '8' THEN '8 - Thin File with Fraud/Verification Product'
    WHEN equifax_hit_code = '9' THEN '9 - No-Hit/Auto-DTEC'
    WHEN equifax_hit_code = 'A' THEN 'A - Report Frozen'
    WHEN equifax_hit_code = 'C' THEN 'C - No-Hit with Information from Additional Source'
    WHEN equifax_hit_code = 'D' THEN 'D - Manual File with Information from Additional Source'
    WHEN equifax_hit_code = 'E' THEN 'E - Manual Consumer Narrative with Information from Additional Source'
    WHEN equifax_hit_code = 'F' THEN 'F - Referred File with Information from Additional Source'
    WHEN equifax_hit_code = 'G' THEN 'G - Report Frozen with Information from Additional Source'
    WHEN equifax_hit_code = 'I' THEN 'I - Fraud Tag on Report'
    WHEN equifax_hit_code = 'J' THEN 'J - Fraud Tag with Information from Additional Source'
END as equifax_hit_code_detailed,
CASE 
    WHEN equifax:rule_set_passed = 'true' THEN 'Approve' 
    WHEN equifax:rule_set_passed = 'false' THEN 'Reject'
    WHEN equifax_hit_code IN ('4','7','8','9','A','G','I','J') THEN 'Reject'
    WHEN parse_json(status_reason):denyApplication = 'true' and parse_json(status_reason):provider = 'equifax' THEN 'Reject'
END AS credit_decision,
CAST(NULL AS STRING) as credit_reject_reason,
equifax:equifax_30d_pd_6m::number as dq30plus_past_due_6m,
equifax:equifax_major_derogatories_6m::number as major_derogatories_6m,
equifax:equifax_non_discharged_bankruptcy::number as non_discharged_bankruptcy,
equifax:equifax_non_util_inquiries_6m::number as non_util_inquiries_6m,
equifax:equifax_revolving_and_unsec_instl_debt_balance::number as rev_and_unsec_instl_debt_balance,
equifax:equifax_bankcard_debt_balance::number as credit_card_debt_balance,
equifax:equifax_revolving_trades_opened_1m::number as revolving_trades_opened_1m,
equifax:equifax_tot_inquiries_6m::number as total_inquiries_6m,
equifax:equifax_tot_pd_trades::number as total_past_due_trades,
equifax:equifax_tot_trades_opened_1m::number as total_trades_opened_1m,
equifax:equifax_total_credit_utilization::number as total_credit_util,
equifax:equifax_num_revolving_60plusd_dq_24m::number as num_revolving_60plusd_dq_24m,
equifax:equifax_num_trades_120plusd_dq_6m as num_trades_120plusd_dq_6m,
equifax:equifax_tot_past_due_revolving::number as total_past_due_rev_trades,
equifax:equifax_total_open_revolving_credit as total_open_revolving_credit,
equifax:equifax_unpaid_third_party_collections_12m::number as unpaid_collections_12m,
equifax:equifax_unpaid_third_party_collections_bal_12m::number as unpaid_collections_bal_12m,
plaid:reference_id::string as plaid_reference_id,
rules_versions:plaid_rules_version::string as income_rules_version,
CASE WHEN plaid:rule_set_passed = 'true' THEN 'Approve' WHEN plaid:rule_set_passed = 'false' THEN 'Reject' END AS income_decision,
CAST(NULL AS STRING) as income_reject_reason,
plaid:plaid_avg_income_2m::decimal(12,2) as avg_income_2m,
plaid:plaid_avg_income_6m::decimal(12,2) as avg_income_6m,
plaid:plaid_avg_inflow_6m::decimal(12,2) as avg_inflow_6m,
plaid:plaid_equifax_debt_to_income_ratio::decimal(5,4) as debt_to_income,
plaid:plaid_tot_income_1m::decimal(12,2) as tot_income_1m,
plaid:plaid_tot_income_6m::decimal(12,2) as tot_income_6m,
plaid:plaid_total_balance::decimal(12,2) as total_balance,
plaid:plaid_neg_bal_occurences_30d as neg_bal_occurences_30d,
CASE 
    WHEN parse_json(status_reason):runModelScoreRulesEngineResult:waitForManualApproval = 'true' 
        AND parse_json(status_reason):runModelScoreRulesEngineResult:passed = 'true' THEN 'Manual Approval'
    WHEN parse_json(status_reason):denyApplication = 'true' and parse_json(status_reason):provider = 'manualApproval' THEN 'Manual Reject'
    WHEN b.onboardingstatus = 'ManualDecline' THEN 'Manual Reject'
    WHEN parse_json(status_reason):runModelScoreRulesEngineResult:passed = 'true' THEN 'Approve' 
    WHEN parse_json(status_reason):runModelScoreRulesEngineResult:passed = 'false' THEN 'Reject'
    WHEN b.onboardingstatus = 'WaitForManualApproval' THEN 'Pending Manual Review'
    WHEN parse_json(status_reason):denyApplication = 'true' and parse_json(status_reason):provider = 'model' THEN 'Reject'
END as model_decision,
model_output:version::string as arro_risk_model_version,
model_output:pd::decimal(11,10) AS arro_risk_model_1_score,
model_output:feature_highest_shap_1_analysis::string AS highest_shap_feature_1,
model_output:feature_highest_shap_2_analysis::string AS highest_shap_feature_2,
model_output:feature_highest_shap_3_analysis::string AS highest_shap_feature_3,
model_output:feature_highest_shap_4_analysis::string AS highest_shap_feature_4,
model_output:feature_highest_shap_5_analysis::string AS highest_shap_feature_5,
approval_testing_random_number,
CASE 
    WHEN application_start_ts::date <= '2023-06-05' THEN CASE
        WHEN approval_testing_random_number > 90 and arro_risk_model_1_score > 0.2 THEN 'Y' 
        WHEN approval_testing_random_number <= 90 and arro_risk_model_1_score > 0.2 THEN 'N'
    END
    WHEN coalesce(rules_versions:credit_policy_version::date,'2023-01-01') < '2023-06-26' THEN CASE
        WHEN approval_testing_random_number > 95 and arro_risk_model_1_score between 0.35 and 0.5 THEN 'Y' 
        WHEN approval_testing_random_number <= 95 and arro_risk_model_1_score between 0.35 and 0.5 THEN 'N'
    END
    WHEN coalesce(rules_versions:credit_policy_version::date,'2023-01-01') >= '2023-06-26' THEN CASE
        WHEN approval_testing_random_number > 95 and arro_risk_model_1_score between 0.32 and 0.4 THEN 'Y'
        WHEN approval_testing_random_number > 95 and arro_risk_model_1_score between 0.2 and 0.32
            and (num_revolving_60plusd_dq_24m > 10 OR total_past_due_rev_trades > 2000 OR neg_bal_occurences_30d > 10) THEN 'Y'
        WHEN approval_testing_random_number <= 95 and arro_risk_model_1_score between 0.32 and 0.4 THEN 'N'
        WHEN approval_testing_random_number <= 95 and arro_risk_model_1_score between 0.2 and 0.32
            and (num_revolving_60plusd_dq_24m > 10 OR total_past_due_rev_trades > 2000 OR neg_bal_occurences_30d > 10) THEN 'N'
    END
END AS approval_test_ind,
rules_versions:terms_assignment_rules_version::string as terms_assignment_rules_version,
line_testing_random_number,
CASE WHEN line_testing_random_number > 50 THEN 'Y' WHEN line_testing_random_number <= 50 THEN 'N' END AS line_test_ind,
COALESCE(line_assignment_random_number, random_number) as line_assignment_random_number,
CASE
    WHEN arro_risk_model_1_score <= 0.03 THEN 200
    WHEN arro_risk_model_1_score <= 0.13 THEN 100
    WHEN arro_risk_model_1_score <= 0.2 THEN 50
    WHEN arro_risk_model_1_score > 0.2 THEN 0
END AS rollout_line_assignment,
COALESCE(CAST(credit_limit as number),parse_json(status_reason):determineCreditLineAndAprResult:creditLimitAmount::number) as initial_credit_limit,
COALESCE(CAST(base_interest_rate as decimal(4,3)), parse_json(status_reason):determineCreditLineAndAprResult:interestRate::decimal(4,3)) as base_interest_rate
FROM {{source('galileo_events_postgres_public','Users')}} b
LEFT JOIN {{source('application_summary','arro-prod-arrosharedres-dynamoDBStack-applications239CE304-T57ARJKCUKJO')}} a
    ON a.owner_id = b.id
WHERE COALESCE(a.application_id,'') NOT IN ('934a77dc-5972-4981-8b0e-28133191c904_d7db4a0a-950b-4a49-903a-c0f1294079e7')
order by application_start_ts DESC;

UPDATE credit_secured.application.application_summary a
SET reject_reason = CASE 
    WHEN a.application_status NOT IN ('declined-identity','failed') THEN a.application_status
    WHEN a.global_watchlist_reason_codes ILIKE '%R186%' THEN 'OFAC'
    WHEN b.kyc_reject_reason ilike '%,%' THEN 'Multiple KYC'
    WHEN b.credit_reject_reason ilike '%,%' THEN 'Multiple Credit'
    WHEN b.income_reject_reason ilike '%,%' THEN 'Multiple Income'
    ELSE COALESCE(b.kyc_reject_reason, b.credit_reject_reason, b.income_reject_reason, 
    CASE WHEN a.kyc_decision = 'Approve' AND b.kyc_decision ILIKE '%DOCV%' AND b.socure_reject = 'Y' THEN 'DocV' END,
    CASE WHEN a.credit_decision = 'Reject' AND a.equifax_hit_code IN ('A','G') THEN 'Frozen Credit' END,
    CASE WHEN a.credit_decision = 'Reject' AND a.equifax_hit_code IN ('4','I','J') THEN 'Fraud Tag on Credit Report' END,
    CASE WHEN a.credit_decision = 'Reject' AND b.credit_reject_reason IS NULL THEN 'Frozen Credit' END,
    CASE WHEN a.model_decision = 'Reject' THEN 'Model' END,
    CASE WHEN a.model_decision = 'Manual Reject' THEN 'Manual Reject' END)
END,
kyc_reject_reason = CASE WHEN a.global_watchlist_reason_codes ILIKE '%R186%' THEN 'OFAC' ELSE b.kyc_reject_reason END,
credit_reject_reason = b.credit_reject_reason,
income_reject_reason = b.income_reject_reason,
docv_ind = CASE WHEN a.kyc_decision = 'Approve' AND b.kyc_decision ILIKE '%DOCV%' THEN 'Y' ELSE 'N' END
FROM (
    SELECT application_id,
    NULLIF(LISTAGG(socure_reject_reason,', '),'') as kyc_reject_reason,
    LISTAGG(socure_decision,', ') as kyc_decision,
    MAX(socure_reject) AS socure_reject,
    NULLIF(LISTAGG(equifax_reject_reason,', '),'') as credit_reject_reason,
    NULLIF(LISTAGG(plaid_reject_reason,', '),'') as income_reject_reason
    FROM (
        SELECT driver.application_id,
        plaid.value:params:"Reject Reason" as plaid_reject_reason,
        socure.value:params:"KYC Decision" as socure_decision,
        socure.value:params:"Reject Reason" as socure_reject_reason,
        CASE WHEN parse_json(status_reason):denyApplication = 'true' and parse_json(status_reason):provider = 'socure' THEN 'Y' ELSE 'N' END AS socure_reject,
        equifax.value:params:"Reject Reason" as equifax_reject_reason
        FROM "RAW_DATA"."APPLICATION_SUMMARY"."ARRO_PROD_ARROSHAREDRES_DYNAMO_DBSTACK_APPLICATIONS_239_CE_304_T_57_ARJKCUKJO" driver
        ,lateral flatten(input => socure:rule_set_decision_reason, outer=> true) socure
        ,lateral flatten(input => equifax:rule_set_decision_reason, outer=> true) equifax
        ,lateral flatten(input => plaid:rule_set_decision_reason, outer=> true) plaid
        GROUP BY 1,2,3,4,5,6
        ) c
    GROUP BY 1
) b
WHERE a.application_id = b.application_id;

UPDATE credit_secured.application.application_summary a
SET detailed_application_status = CASE
    WHEN application_status = 'approved' AND model_decision = 'Manual Approval' THEN 'Manual Approved'
    WHEN application_status = 'approved' THEN 'Approved'
    WHEN model_decision = 'Pending Manual Review' THEN 'Manual Review'
    WHEN model_decision IN ('Approve','Manual Approval') THEN 'Pending CHA'
    WHEN coalesce(reject_reason,'failed') NOT IN ('failed','started','aborted') THEN reject_reason
    WHEN income_reject_reason IS NOT NULL AND income_reject_reason ilike '%,%' THEN 'Multiple Income'
    WHEN income_reject_reason IS NOT NULL THEN income_reject_reason
    WHEN credit_decision = 'Approve' and income_decision IS NULL THEN 'Pending Plaid'
    WHEN kyc_decision = 'Approve' and docv_ind = 'Y' and credit_decision IS NULL THEN 'Pending DocV'
    WHEN kyc_decision = 'Approve' and credit_decision IS NULL THEN 'Equifax Error'
    WHEN application_id IS NULL THEN 'Pre-KYC Drop Off'
    WHEN current_onboarding_status = 'CustomerOnboardingExecutionFailed' THEN 'Execution Error'
    WHEN current_onboarding_status = 'RequestUserConsentToCreateAccount' AND model_decision = 'Manual Approval'  THEN 'Manual Approval Pending CHA'
    WHEN current_onboarding_status = 'RequestUserConsentToCreateAccount' THEN 'Pending CHA'
END;

RETURN 'Success';
END
$$
;

CREATE OR REPLACE PROCEDURE credit.application.application_summary_flattening_procedure()
  RETURNS VARCHAR
  LANGUAGE SQL
  AS
  $$
BEGIN
create or replace table credit.application.application_summary as
SELECT
COALESCE(a.owner_id, b.id) as user_id,
a.application_id as application_id,
current_timestamp as last_update_ts,
CONVERT_TIMEZONE('America/Los_Angeles',_CT) AS application_start_ts,
CONVERT_TIMEZONE('America/Los_Angeles', b.createdAt) as profile_creation_ts,
b.onboardingstatus as current_onboarding_status,
COALESCE(CONVERT_TIMEZONE('America/Los_Angeles',a.STATUS_UPDATED_AT),CONVERT_TIMEZONE('America/Los_Angeles',b.updatedat)) AS last_status_update_ts,
ROW_NUMBER () OVER (PARTITION BY user_id ORDER BY last_status_update_ts DESC) AS application_recency,
--Unsecured Info
'Mobile' AS application_source,
CASE 
    WHEN USER_ID IN ('ce3cda02-5c63-4caa-a16d-068c7f3b9fc1','9acde48c-f06b-495d-8531-15b672f5a1fc','bcf80dec-5e09-463e-b90b-797209561f0a','74e4e738-4786-4ee1-a19b-42f0c73a7082','79904310-5158-430b-abdd-292d3fe3b385','e8a36bcd-4373-49f7-b70f-6c6a311f154d','fce25b99-c0d1-4427-9efa-0e04e0eddeba','f0699438-31b6-4942-9e84-cdfe29b4157c','49babc6b-18ae-45d9-bb1d-65543b916cbe','6f2e2f33-5f8b-469e-9a45-ae85f98198c5','aed4d33b-e082-4c8a-83c9-ff6a0a2faa93','e0373c7f-e8c4-441e-84d8-c1ce4802bc42','b3547996-b1d3-49d4-ac4a-bfff63100324','5dfd9602-30ad-4dd4-8994-8aedf4def179','8a928404-528d-445d-a371-62a9c7ffaf2d','b4ca0f90-9016-4dfd-b183-1975de7f7b7a','f4fd110b-6b49-45f5-84d8-1cbf6615f733','3fcb09b7-f3de-4ac5-8b07-d5d133b788a2','1fbd1e1b-ac61-4b93-8167-d0490dc35663') THEN 'Alpha'
    WHEN USER_ID IN ('ea8c29ab-9de8-48bd-ba17-4f7b187a10e7','7d3bdb8f-63e1-459d-89db-8936b8e45ee1','7bd28a56-b4bf-414a-988f-0cb318591a59','0813f141-ba10-4827-a74a-4dec72193a6f','e1a927df-1be9-457b-9b01-d1f57dc8d239','89e77c4d-2922-438f-97b0-e7ba1af7d6ad','05f2a385-85ec-470d-9188-cd8ae7685640','a1f591ea-acdc-4c05-bd49-8755cf9ea916','3547bb94-6f19-4d42-b2db-ddfb29f1e35e','fa6975e1-4484-4728-adfe-a45d1c5b637d','8c14f93e-902c-4988-9b97-ffa9421212af','afa75382-e22d-4427-8689-33d546c183ff','37b7d3bd-519b-46d6-ae6c-d0ddad771732','f38197fa-f9b0-4657-a69a-32bb2c1adfde','6242b088-c772-4a48-8394-4ae736e3f93e','375865e1-5ed3-4fb6-af08-376971d5271f','461ed6d2-cc05-4ff8-a3d7-24346eeeecc6','82d6ca94-310d-4753-bcb9-9b367c3b6d41','16dd9a60-9b50-41a8-87b1-68727bf339a5','10e0546e-be22-4686-bcbf-57ddd4f3ec69','50a2192c-1135-40c3-9bdd-9a728b62142c','36cb00b9-d860-4de8-96a1-2d826eaae05c','2a598480-1f43-46a1-9f21-2a1bdefe9cb8','bd07aa00-d234-4b91-b20b-6875d0a1759e','94fd0bcd-86ac-4478-8100-a8a02bc8627c','eade2a84-96e9-4d36-a166-b87412e975f4','efa71065-d49a-443b-a171-7f9661ce681a','0fd6530e-65fb-4006-b1d7-3b27d96c5566') THEN 'Beta'
    WHEN USER_ID IN ('1217ea54-d71f-4cae-bb04-f483d6ab0a7a','9863bdeb-a083-40db-af39-bde65f973672','d9c3eb21-c630-4b4f-925d-2ab0bc32af34','162b623b-75d6-47c0-80bb-d573b89089fb','770bc8ce-61fa-4f2e-a275-652badcfbe44','74b9a553-3b31-4ba2-9f1e-71530ede4557','36823cfb-ae29-4cb8-ad8a-573bae5be057','2e1715e1-ef66-4f3a-91fd-97429c2a31a7','c01269e3-4948-486a-b19b-351336b44179','4ef87d78-6eb9-4d54-9668-6cd84e4a07be','38f15f50-806a-415e-a30a-6f3d89568d70','8bcfe448-f606-4ca7-82ed-3cf31096c388','b974be94-7e52-4616-bac9-36e9dfea5e27','cb4068cf-4ea2-4bcf-ad4e-0d3022825caa','08f24240-8d8c-4ca0-add2-767565924daa','1ca61d60-0fff-48e4-b3b0-ab97f4f66fe5','e2b954d8-72f3-42e4-b42a-bfd903cfc2ef','6c2c9c70-d083-47a9-8a46-8e5a0559e412','40586f92-7467-4e0a-899d-35e78b3aa045','b201151a-b507-4343-a282-a2343ce14f5e') Then 'Confirmed Fraud'
    ELSE 'Rollout'
END AS testing_stage,
rules_versions:credit_policy_version::string as credit_policy_version,
profile:waitList::string as waitlist_ind,
CASE WHEN b.onboardingstatus = 'ManualDecline' then 'failed' ELSE status end as application_status,
CAST(NULL AS STRING) AS detailed_application_status,
CAST(NULL AS STRING) as reject_reason,
socure:reference_id::string as socure_reference_id,
rules_versions:socure_rules_version::string as kyc_rules_version,
CASE WHEN socure:rule_set_passed = 'true' THEN 'Approve' WHEN socure:rule_set_passed = 'false' THEN 'Reject' END AS kyc_decision,
CAST(NULL AS STRING) as kyc_reject_reason,
CAST(NULL AS CHAR(1)) AS docv_ind,
socure:socure_address_risk_reason_codes::string as address_risk_reason_codes,
socure:socure_address_risk_score::decimal(4,3) as address_risk_score,
socure:socure_alert_risk_reason_codes::string as alert_risk_reason_codes,
socure:socure_email_risk_reason_codes::string as email_risk_reason_codes,
socure:socure_email_risk_score::decimal(4,3) as email_risk_score,
socure:socure_global_watchlist_reason_codes::string as global_watchlist_reason_codes,
socure:socure_kyc_field_validation_first_name::decimal(3,2) as kyc_field_validation_first_name,
socure:socure_kyc_field_validation_sur_name::decimal(3,2) as kyc_field_validation_surname,
socure:socure_kyc_field_validation_mobile_number::decimal(3,2) as kyc_field_validation_mobile_number,
socure:socure_kyc_field_validation_dob::decimal(3,2) as kyc_field_validation_dob,
socure:socure_kyc_field_validation_ssn::decimal(3,2) as kyc_field_validation_ssn,
socure:socure_kyc_field_validation_street_address::decimal(3,2) as kyc_field_validation_street_address,
socure:socure_kyc_field_validation_city::decimal(3,2) as kyc_field_validation_city,
socure:socure_kyc_field_validation_state::decimal(3,2) as kyc_field_validation_state,
socure:socure_kyc_field_validation_zip::decimal(3,2) as kyc_field_validation_zip,
socure:socure_kyc_reason_codes::string as kyc_reason_codes,
socure:socure_phone_risk_reason_codes::string as phone_risk_reason_codes,
socure:socure_phone_risk_score::decimal(4,3) as phone_risk_score,
socure:socure_sigma_identity_fraud_reason_codes::string as identity_fraud_reason_codes,
socure:socure_sigma_identity_fraud_score::decimal(4,3) as identity_fraud_score,
socure:socure_sigma_synthetic_fraud_reason_codes::string as synthetic_fraud_reason_codes,
socure:socure_sigma_synthetic_fraud_score::decimal(4,3) as synthetic_fraud_score,
equifax:reference_id::string as equifax_reference_id,
rules_versions:equifax_rules_version::string as credit_rules_version,
equifax_hit_code,
CASE 
    WHEN equifax_hit_code = '1' THEN '1 - Hit'
    WHEN equifax_hit_code = '2' THEN '2 - No-Hit'
    WHEN equifax_hit_code = '3' THEN '3 - Manual File'
    WHEN equifax_hit_code = '4' THEN '4 - Manual File Review Required'
    WHEN equifax_hit_code = '5' THEN '5 - Referred File'
    WHEN equifax_hit_code = '6' THEN '6 - Hit with Automated Consumer Narrative'
    WHEN equifax_hit_code = '7' THEN '7 - Fraud/Verification Product'
    WHEN equifax_hit_code = '8' THEN '8 - Thin File with Fraud/Verification Product'
    WHEN equifax_hit_code = '9' THEN '9 - No-Hit/Auto-DTEC'
    WHEN equifax_hit_code = 'A' THEN 'A - Report Frozen'
    WHEN equifax_hit_code = 'C' THEN 'C - No-Hit with Information from Additional Source'
    WHEN equifax_hit_code = 'D' THEN 'D - Manual File with Information from Additional Source'
    WHEN equifax_hit_code = 'E' THEN 'E - Manual Consumer Narrative with Information from Additional Source'
    WHEN equifax_hit_code = 'F' THEN 'F - Referred File with Information from Additional Source'
    WHEN equifax_hit_code = 'G' THEN 'G - Report Frozen with Information from Additional Source'
    WHEN equifax_hit_code = 'I' THEN 'I - Fraud Tag on Report'
    WHEN equifax_hit_code = 'J' THEN 'J - Fraud Tag with Information from Additional Source'
END as equifax_hit_code_detailed,
CASE 
    WHEN equifax:rule_set_passed = 'true' THEN 'Approve' 
    WHEN equifax:rule_set_passed = 'false' THEN 'Reject'
    WHEN equifax_hit_code IN ('4','7','8','9','A','G','I','J') THEN 'Reject'
    WHEN parse_json(status_reason):denyApplication = 'true' and parse_json(status_reason):provider = 'equifax' THEN 'Reject'
END AS credit_decision,
CAST(NULL AS STRING) as credit_reject_reason,
equifax:equifax_30d_pd_6m::number as dq30plus_past_due_6m,
equifax:equifax_major_derogatories_6m::number as major_derogatories_6m,
equifax:equifax_non_discharged_bankruptcy::number as non_discharged_bankruptcy,
equifax:equifax_non_util_inquiries_6m::number as non_util_inquiries_6m,
equifax:equifax_revolving_and_unsec_instl_debt_balance::number as rev_and_unsec_instl_debt_balance,
equifax:equifax_bankcard_debt_balance::number as credit_card_debt_balance,
equifax:equifax_revolving_trades_opened_1m::number as revolving_trades_opened_1m,
equifax:equifax_tot_inquiries_6m::number as total_inquiries_6m,
equifax:equifax_tot_pd_trades::number as total_past_due_trades,
equifax:equifax_tot_trades_opened_1m::number as total_trades_opened_1m,
equifax:equifax_total_credit_utilization::number as total_credit_util,
equifax:equifax_num_revolving_60plusd_dq_24m::number as num_revolving_60plusd_dq_24m,
equifax:equifax_num_trades_120plusd_dq_6m as num_trades_120plusd_dq_6m,
equifax:equifax_tot_past_due_revolving::number as total_past_due_rev_trades,
equifax:equifax_total_open_revolving_credit as total_open_revolving_credit,
equifax:equifax_unpaid_third_party_collections_12m::number as unpaid_collections_12m,
equifax:equifax_unpaid_third_party_collections_bal_12m::number as unpaid_collections_bal_12m,
plaid:reference_id::string as plaid_reference_id,
rules_versions:plaid_rules_version::string as income_rules_version,
CASE WHEN plaid:rule_set_passed = 'true' THEN 'Approve' WHEN plaid:rule_set_passed = 'false' THEN 'Reject' END AS income_decision,
CAST(NULL AS STRING) as income_reject_reason,
plaid:plaid_avg_income_2m::decimal(12,2) as avg_income_2m,
plaid:plaid_avg_income_6m::decimal(12,2) as avg_income_6m,
plaid:plaid_avg_inflow_6m::decimal(12,2) as avg_inflow_6m,
plaid:plaid_equifax_debt_to_income_ratio::decimal(5,4) as debt_to_income,
plaid:plaid_tot_income_1m::decimal(12,2) as tot_income_1m,
plaid:plaid_tot_income_6m::decimal(12,2) as tot_income_6m,
plaid:plaid_total_balance::decimal(12,2) as total_balance,
plaid:plaid_neg_bal_occurences_30d as neg_bal_occurences_30d,
CASE 
    WHEN parse_json(status_reason):runModelScoreRulesEngineResult:waitForManualApproval = 'true' 
        AND parse_json(status_reason):runModelScoreRulesEngineResult:passed = 'true' THEN 'Manual Approval'
    WHEN parse_json(status_reason):denyApplication = 'true' and parse_json(status_reason):provider = 'manualApproval' THEN 'Manual Reject'
    WHEN b.onboardingstatus = 'ManualDecline' THEN 'Manual Reject'
    WHEN parse_json(status_reason):runModelScoreRulesEngineResult:passed = 'true' THEN 'Approve' 
    WHEN parse_json(status_reason):runModelScoreRulesEngineResult:passed = 'false' THEN 'Reject'
    WHEN b.onboardingstatus = 'WaitForManualApproval' THEN 'Pending Manual Review'
    WHEN parse_json(status_reason):denyApplication = 'true' and parse_json(status_reason):provider = 'model' THEN 'Reject'
END as model_decision,
model_output:version::string as arro_risk_model_version,
model_output:pd::decimal(11,10) AS arro_risk_model_1_score,
model_output:feature_highest_shap_1_analysis::string AS highest_shap_feature_1,
model_output:feature_highest_shap_2_analysis::string AS highest_shap_feature_2,
model_output:feature_highest_shap_3_analysis::string AS highest_shap_feature_3,
model_output:feature_highest_shap_4_analysis::string AS highest_shap_feature_4,
model_output:feature_highest_shap_5_analysis::string AS highest_shap_feature_5,
approval_testing_random_number,
CASE 
    WHEN application_start_ts::date <= '2023-06-05' THEN CASE
        WHEN approval_testing_random_number > 90 and arro_risk_model_1_score > 0.2 THEN 'Y' 
        WHEN approval_testing_random_number <= 90 and arro_risk_model_1_score > 0.2 THEN 'N'
    END
    WHEN coalesce(rules_versions:credit_policy_version::date,'2023-01-01') < '2023-06-26' THEN CASE
        WHEN approval_testing_random_number > 95 and arro_risk_model_1_score between 0.35 and 0.5 THEN 'Y' 
        WHEN approval_testing_random_number <= 95 and arro_risk_model_1_score between 0.35 and 0.5 THEN 'N'
    END
    WHEN coalesce(rules_versions:credit_policy_version::date,'2023-01-01') >= '2023-06-26' THEN CASE
        WHEN approval_testing_random_number > 95 and arro_risk_model_1_score between 0.32 and 0.4 THEN 'Y'
        WHEN approval_testing_random_number > 95 and arro_risk_model_1_score between 0.2 and 0.32
            and (num_revolving_60plusd_dq_24m > 10 OR total_past_due_rev_trades > 2000 OR neg_bal_occurences_30d > 10) THEN 'Y'
        WHEN approval_testing_random_number <= 95 and arro_risk_model_1_score between 0.32 and 0.4 THEN 'N'
        WHEN approval_testing_random_number <= 95 and arro_risk_model_1_score between 0.2 and 0.32
            and (num_revolving_60plusd_dq_24m > 10 OR total_past_due_rev_trades > 2000 OR neg_bal_occurences_30d > 10) THEN 'N'
    END
END AS approval_test_ind,
rules_versions:terms_assignment_rules_version::string as terms_assignment_rules_version,
line_testing_random_number,
CASE WHEN line_testing_random_number > 50 THEN 'Y' WHEN line_testing_random_number <= 50 THEN 'N' END AS line_test_ind,
COALESCE(line_assignment_random_number, random_number) as line_assignment_random_number,
CASE
    WHEN arro_risk_model_1_score <= 0.03 THEN 200
    WHEN arro_risk_model_1_score <= 0.13 THEN 100
    WHEN arro_risk_model_1_score <= 0.2 THEN 50
    WHEN arro_risk_model_1_score > 0.2 THEN 0
END AS rollout_line_assignment,
COALESCE(CAST(credit_limit as number),parse_json(status_reason):determineCreditLineAndAprResult:creditLimitAmount::number) as initial_credit_limit,
COALESCE(CAST(base_interest_rate as decimal(4,3)), parse_json(status_reason):determineCreditLineAndAprResult:interestRate::decimal(4,3)) as base_interest_rate
FROM {{source('galileo_events_postgres_public','Users')}} b
LEFT JOIN {{source('application_summary','arro-prod-arrosharedres-dynamoDBStack-applications239CE304-T57ARJKCUKJO')}} a
    ON a.owner_id = b.id
WHERE COALESCE(a.application_id,'') NOT IN ('934a77dc-5972-4981-8b0e-28133191c904_d7db4a0a-950b-4a49-903a-c0f1294079e7')
order by application_start_ts DESC;

UPDATE credit.application.application_summary a
SET reject_reason = CASE 
    WHEN a.application_status NOT IN ('declined-identity','failed') THEN a.application_status
    WHEN a.global_watchlist_reason_codes ILIKE '%R186%' THEN 'OFAC'
    WHEN b.kyc_reject_reason ilike '%,%' THEN 'Multiple KYC'
    WHEN b.credit_reject_reason ilike '%,%' THEN 'Multiple Credit'
    WHEN b.income_reject_reason ilike '%,%' THEN 'Multiple Income'
    ELSE COALESCE(b.kyc_reject_reason, b.credit_reject_reason, b.income_reject_reason, 
    CASE WHEN a.kyc_decision = 'Approve' AND b.kyc_decision ILIKE '%DOCV%' AND b.socure_reject = 'Y' THEN 'DocV' END,
    CASE WHEN a.credit_decision = 'Reject' AND a.equifax_hit_code IN ('A','G') THEN 'Frozen Credit' END,
    CASE WHEN a.credit_decision = 'Reject' AND a.equifax_hit_code IN ('4','I','J') THEN 'Fraud Tag on Credit Report' END,
    CASE WHEN a.credit_decision = 'Reject' AND b.credit_reject_reason IS NULL THEN 'Frozen Credit' END,
    CASE WHEN a.model_decision = 'Reject' THEN 'Model' END,
    CASE WHEN a.model_decision = 'Manual Reject' THEN 'Manual Reject' END)
END,
kyc_reject_reason = CASE WHEN a.global_watchlist_reason_codes ILIKE '%R186%' THEN 'OFAC' ELSE b.kyc_reject_reason END,
credit_reject_reason = b.credit_reject_reason,
income_reject_reason = b.income_reject_reason,
docv_ind = CASE WHEN a.kyc_decision = 'Approve' AND b.kyc_decision ILIKE '%DOCV%' THEN 'Y' ELSE 'N' END
FROM (
    SELECT application_id,
    NULLIF(LISTAGG(socure_reject_reason,', '),'') as kyc_reject_reason,
    LISTAGG(socure_decision,', ') as kyc_decision,
    MAX(socure_reject) AS socure_reject,
    NULLIF(LISTAGG(equifax_reject_reason,', '),'') as credit_reject_reason,
    NULLIF(LISTAGG(plaid_reject_reason,', '),'') as income_reject_reason
    FROM (
        SELECT driver.application_id,
        plaid.value:params:"Reject Reason" as plaid_reject_reason,
        socure.value:params:"KYC Decision" as socure_decision,
        socure.value:params:"Reject Reason" as socure_reject_reason,
        CASE WHEN parse_json(status_reason):denyApplication = 'true' and parse_json(status_reason):provider = 'socure' THEN 'Y' ELSE 'N' END AS socure_reject,
        equifax.value:params:"Reject Reason" as equifax_reject_reason
        FROM "RAW_DATA"."APPLICATION_SUMMARY"."ARRO_PROD_ARROSHAREDRES_DYNAMO_DBSTACK_APPLICATIONS_239_CE_304_T_57_ARJKCUKJO" driver
        ,lateral flatten(input => socure:rule_set_decision_reason, outer=> true) socure
        ,lateral flatten(input => equifax:rule_set_decision_reason, outer=> true) equifax
        ,lateral flatten(input => plaid:rule_set_decision_reason, outer=> true) plaid
        GROUP BY 1,2,3,4,5,6
        ) c
    GROUP BY 1
) b
WHERE a.application_id = b.application_id;

UPDATE credit.application.application_summary a
SET detailed_application_status = CASE
    WHEN application_status = 'approved' AND model_decision = 'Manual Approval' THEN 'Manual Approved'
    WHEN application_status = 'approved' THEN 'Approved'
    WHEN model_decision = 'Pending Manual Review' THEN 'Manual Review'
    WHEN model_decision IN ('Approve','Manual Approval') THEN 'Pending CHA'
    WHEN coalesce(reject_reason,'failed') NOT IN ('failed','started','aborted') THEN reject_reason
    WHEN income_reject_reason IS NOT NULL AND income_reject_reason ilike '%,%' THEN 'Multiple Income'
    WHEN income_reject_reason IS NOT NULL THEN income_reject_reason
    WHEN credit_decision = 'Approve' and income_decision IS NULL THEN 'Pending Plaid'
    WHEN kyc_decision = 'Approve' and docv_ind = 'Y' and credit_decision IS NULL THEN 'Pending DocV'
    WHEN kyc_decision = 'Approve' and credit_decision IS NULL THEN 'Equifax Error'
    WHEN application_id IS NULL THEN 'Pre-KYC Drop Off'
    WHEN current_onboarding_status = 'CustomerOnboardingExecutionFailed' THEN 'Execution Error'
    WHEN current_onboarding_status = 'RequestUserConsentToCreateAccount' AND model_decision = 'Manual Approval'  THEN 'Manual Approval Pending CHA'
    WHEN current_onboarding_status = 'RequestUserConsentToCreateAccount' THEN 'Pending CHA'
END;