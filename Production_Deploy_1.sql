-- Semantic views to connect to flattened dimensional table

--Adding Start Time
--Updating latest run stats to apollo control table
UPDATE rpt_apollo.apollo_control_table
set last_start_date = getdate() 
WHERE sql_file_name = :'sqlfilename';


CREATE OR REPLACE VIEW :loadschema.v_tab_Account_Attributes AS
(
SELECT 
account_id,
acc_hcos_id as "hcos Id",
acc_source_unique_id as "Account CDM Id",
acc_subtype_code as "Account CDM Subtype",
acc_type_code as "Account CDM Type",
ahi_lvl7anc_accnt_id as "Facility Id",
ahi_lvl7anc_accnt_name as Facility,
ahi_lvl8anc_accnt_id as "Division Id",
ahi_lvl8anc_accnt_name as Division,
ahi_top_lvl_accnt_id as "IDN Id",
ahi_top_lvl_accnt_name as IDN,
ahi_true_level as "True Level"
FROM :loadschema.ptab_d_account_full acc
--Matching account_id count and not duplicating
where acc_row_number = 1
)
WITH NO SCHEMA BINDING;

/* -- Granting read only access to read group and full control to ETL user

GRANT TRIGGER, RULE, SELECT, DELETE, UPDATE, REFERENCES, INSERT ON :loadschema.v_tab_Account_Attributes TO oasis_cdw_tst_procuser;
GRANT SELECT ON :loadschema.v_tab_Account_Attributes TO group oasis_cdw_tst_readuser_group;


-- Check for Compile of VIEW
SELECT *
FROM :loadschema.v_tab_Account_Attributes
LIMIT 1; */


CREATE OR REPLACE VIEW :loadschema.v_tab_Account_with_Affiliated_Attributes AS
(SELECT DISTINCT
"Highest Affiliation Level",
acc.aff_customer_id as customer_id
FROM  :loadschema.ptab_d_account_full acc
WHERE aff_customer_id is not null
)
WITH NO SCHEMA BINDING;

/* -- Granting read only access to read group and full control to ETL user

GRANT TRIGGER, RULE, SELECT, DELETE, UPDATE, REFERENCES, INSERT ON :loadschema.v_tab_Account_with_Affiliated_Attributes TO oasis_cdw_tst_procuser;
GRANT SELECT ON :loadschema.v_tab_Account_with_Affiliated_Attributes TO group oasis_cdw_tst_readuser_group;

-- Check for Compile of VIEW
SELECT *
FROM :loadschema.v_tab_Account_with_Affiliated_Attributes
LIMIT 1; */

CREATE OR REPLACE VIEW :loadschema.v_tab_Account_with_Affiliated_Facility_Attributes AS
(SELECT 
account_id,
acc_hcos_id as "Facility hcos Id",
acc_source_unique_id as "Facility CDM Id",
acc_subtype_code as "Facility CDM Subtype",
acc_type_code as "Facility CDM Type",
CASE 
WHEN ahi_lvl8anc_accnt_name = 'UNASSIGNED'  THEN ahi_top_lvl_accnt_ID
WHEN ahi_lvl8anc_accnt_name = 'ORG_UNASSIGNED' THEN '-9999990'
else ahi_lvl8anc_accnt_id end as "Division Id",
ahi_lvl8anc_accnt_name as Division,
ahi_lvl7anc_accnt_id as "Facility Id",
ahi_lvl7anc_accnt_name as Facility,
aff_customer_id as customer_id,
affiliation_deduplication_id
FROM  :loadschema.ptab_d_account_full acc
where aff_level in ('FACILITY','UNAFFILIATED FACILITY')
)
WITH NO SCHEMA BINDING;

/* -- Granting read only access to read group and full control to ETL user

GRANT TRIGGER, RULE, SELECT, DELETE, UPDATE, REFERENCES, INSERT ON :loadschema.v_tab_Account_with_Affiliated_Facility_Attributes TO oasis_cdw_tst_procuser;
GRANT SELECT ON :loadschema.v_tab_Account_with_Affiliated_Facility_Attributes TO group oasis_cdw_tst_readuser_group;

-- Check for Compile of VIEW
SELECT *
FROM :loadschema.v_tab_Account_with_Affiliated_Facility_Attributes
LIMIT 1;
 */

CREATE OR REPLACE VIEW :loadschema.v_tab_Account_with_Affiliated_IDN_Attributes AS
(SELECT 
account_id,
acc_hcos_id as "IDN hcos Id",
acc_source_unique_id as "IDN CDM Id",
acc_subtype_code as "IDN CDM Subtype",
acc_type_code as "IDN CDM Type",
ahi_top_lvl_accnt_id as "IDN Id",
ahi_top_lvl_accnt_name as IDN,
aff_customer_id as customer_id,
affiliation_deduplication_id
FROM  :loadschema.ptab_d_account_full acc
where aff_level in ('CORP PARENT','UNAFFILIATED CORP PARENT')
union all
SELECT
account_id,
'-1' as "IDN hcos Id",
'-1' as "IDN CDM Id",
'UNASSIGNED' as "IDN CDM Subtype",
'UNASSIGNED' as "IDN CDM Type",
ahi_top_lvl_accnt_id  as "IDN Id",
ahi_top_lvl_accnt_name as IDN,
acc.aff_customer_id as customer_id,
affiliation_deduplication_id
FROM  :loadschema.ptab_d_account_full acc
where aff_level in ('FACILITY')  and ahi_lvl8anc_accnt_name = 'UNASSIGNED' AND ahi_top_lvl_accnt_name = 'UNASSIGNED' 
UNION ALL
SELECT 
account_id,
acc_hcos_id as "Division hcos Id",
acc_source_unique_id as "Division CDM Id",
acc_subtype_code as "Division CDM Subtype",
acc_type_code as "Division CDM Type",
ahi_top_lvl_accnt_id as "IDN Id",
'ORG_UNASSIGNED_SUBDIVISION' as IDN,
acc.aff_customer_id as customer_id,
affiliation_deduplication_id
FROM  :loadschema.ptab_d_account_full acc
where aff_level in ('SUB DIVISION','UNAFFILIATED SUB DIVISION') AND ahi_lvl8anc_accnt_name = 'ORG_UNASSIGNED' AND aff_customer_id not in
(Select aff_customer_id
FROM  :loadschema.ptab_d_account_full acc
where aff_level in ('CORP PARENT','UNAFFILIATED CORP PARENT') AND ahi_lvl8anc_accnt_name = 'ORG_UNASSIGNED')
union all
SELECT 
account_id,
acc_hcos_id as "Division hcos Id",
acc_source_unique_id as "Division CDM Id",
acc_subtype_code as "Division CDM Subtype",
acc_type_code as "Division CDM Type",
ahi_top_lvl_accnt_id as "IDN Id",
'ORG_UNASSIGNED_FACILITY' as IDN,
acc.aff_customer_id as customer_id,
affiliation_deduplication_id
FROM  :loadschema.ptab_d_account_full acc
where aff_level in ('FACILITY','UNAFFILIATED FACILITY') AND ahi_lvl8anc_accnt_name = 'ORG_UNASSIGNED' AND aff_customer_id not in
(Select aff_customer_id
FROM  :loadschema.ptab_d_account_full acc
where aff_level in ('SUB DIVISION','UNAFFILIATED SUB DIVISION','CORP PARENT','UNAFFILIATED CORP PARENT') AND ahi_lvl8anc_accnt_name = 'ORG_UNASSIGNED')
)
WITH NO SCHEMA BINDING;

/* -- Granting read only access to read group and full control to ETL user

GRANT TRIGGER, RULE, SELECT, DELETE, UPDATE, REFERENCES, INSERT ON :loadschema.v_tab_Account_with_Affiliated_IDN_Attributes TO oasis_cdw_tst_procuser;
GRANT SELECT ON :loadschema.v_tab_Account_with_Affiliated_IDN_Attributes TO group oasis_cdw_tst_readuser_group;

-- Check for Compile of VIEW
SELECT *
FROM :loadschema.v_tab_Account_with_Affiliated_IDN_Attributes
LIMIT 1; */


CREATE OR REPLACE VIEW :loadschema.v_tab_Account_with_Affiliated_Subdivision_Attributes AS
(SELECT 
account_id,
acc_hcos_id as "Division hcos Id",
acc_source_unique_id as "Division CDM Id",
acc_subtype_code as "Division CDM Subtype",
acc_type_code as "Division CDM Type",
ahi_lvl8anc_accnt_id as "Division Id",
ahi_lvl8anc_accnt_name as Division,
ahi_top_lvl_accnt_id as "IDN Id",
ahi_top_lvl_accnt_name as IDN,
acc.aff_customer_id as customer_id,
affiliation_deduplication_id
FROM  :loadschema.ptab_d_account_full acc
where aff_level in ('SUB DIVISION','UNAFFILIATED SUB DIVISION')
UNION all
Select
account_id,
'-1' as "Division hcos Id",
'-1' as "Division CDM Id",
'UNASSIGNED' as "Division CDM Subtype",
'UNASSIGNED' as "Division CDM Type",
ahi_top_lvl_accnt_id as "Division Id",
'UNASSIGNED' as Division,
ahi_top_lvl_accnt_id as "IDN Id",
ahi_top_lvl_accnt_name as IDN,
acc.aff_customer_id as customer_id,
affiliation_deduplication_id
FROM  :loadschema.ptab_d_account_full acc
where aff_level in ('FACILITY')  and ahi_lvl8anc_accnt_name = 'UNASSIGNED'
GROUP BY 1,2,3,4,5,6,7,8,9,10,11
UNION ALL
SELECT 
account_id,
acc_hcos_id as "Division hcos Id",
acc_source_unique_id as "Division CDM Id",
acc_subtype_code as "Division CDM Subtype",
acc_type_code as "Division CDM Type",
ahi_lvl8anc_accnt_id as "Division Id",
'ORG_UNASSIGNED_FACILITY' as Division,
ahi_top_lvl_accnt_id as "IDN Id",
'ORG_UNASSIGNED_FACILITY' as IDN,
acc.aff_customer_id as customer_id,
affiliation_deduplication_id
FROM  :loadschema.ptab_d_account_full acc
where aff_level in ('FACILITY','UNAFFILIATED FACILITY') AND ahi_lvl8anc_accnt_name = 'ORG_UNASSIGNED' AND aff_customer_id not in
(Select aff_customer_id
FROM  :loadschema.ptab_d_account_full acc
where aff_level in ('SUB DIVISION','UNAFFILIATED SUB DIVISION') AND ahi_lvl8anc_accnt_name = 'ORG_UNASSIGNED')
)
WITH NO SCHEMA BINDING;

/* -- Granting read only access to read group and full control to ETL user

GRANT TRIGGER, RULE, SELECT, DELETE, UPDATE, REFERENCES, INSERT ON :loadschema.v_tab_Account_with_Affiliated_Subdivision_Attributes TO oasis_cdw_tst_procuser;
GRANT SELECT ON :loadschema.v_tab_Account_with_Affiliated_Subdivision_Attributes TO group oasis_cdw_tst_readuser_group;

-- Check for Compile of VIEW
SELECT *
FROM :loadschema.v_tab_Account_with_Affiliated_Subdivision_Attributes
LIMIT 1; */

DROP VIEW IF EXISTS :loadschema.v_tab_Activity_Data_Calls;

CREATE OR REPLACE VIEW :loadschema.v_tab_Activity_Data_Calls AS
(
SELECT 
CASE
   when datasource_id in ('OSS_CALL_DETAIL', 'VEEVA_CALL_DETAIL') AND call_type IN ('DETAIL ONLY', 'DETAIL WITH SAMPLE', 'GROUP DETAIL', 'GROUP DETAIL WITH SAMPLE')
     then  interaction_call_id                      
   END as Actual_Call,
       is_parent_call as "Is Parent Call",
       detail_priority as "Detail Priority",
       quantity,
	   f.territory_type as "Territory Type" ,
       f.week_ending_date as "Week Ending Date",
       f.day as "Day",
	   f.market as "Market",
	   f.therapeutic_class as "Therapeutic Class",
       f.therapeutic_sub_class as "Therapeutic Sub-Class",
	   CASE
   when datasource_id in ('OSS_CALL_SAMPLE', 'VEEVA_CALL_SAMPLE') AND call_type IN ('DETAIL WITH SAMPLE', 'GROUP DETAIL WITH SAMPLE', 'SAMPLE ONLY')
     then  quantity 
     else 0 
   END as "Sample Qty",
   CASE
   when datasource_id in ('OSS_CALL_DETAIL', 'VEEVA_CALL_DETAIL') AND call_type IN ('DETAIL ONLY', 'DETAIL WITH SAMPLE', 'GROUP DETAIL', 'GROUP DETAIL WITH SAMPLE')
     then  child_ld                      
   END as "Details",
       pre_call_notes as "Pre Call Notes",
       discussion_num_of_attendees as "Discussion # of Attendees",
       display_order as "Display Order",
       presentation_id,
       slide_version as "Slide Version",
       key_message_start_time as "Key Message Start Time",
       call_name as "Call Name",
       interaction_call_id as "Interaction Call Id",
       datasource_id,
       employee_id,
       address_id,
       attendee_type as "Attendee Type",
       call_type as "Call Type",
       interaction_sub_type as "Interaction Sub Type",
       interaction_type as "Interaction Type",
       detail_type as "Detail Type",
       delivery_status as "Delivery Status",
       child_type as "Child Type",
       product_type as "Product Type",
       discussion_topics as "Discussion Topics",
       product_strategy as "Product Strategy",
       clinical_research as "Clinical Research",
       discussion_class as "Discussion Class",
       device_trained as "Device Trained",
       call_focus as "Call Focus",
       category as "Category",
       reaction as "Reaction",
       activity_type as "Activity Type",
       call_status as "Call Status",
       street_address as "Street Address",
       best_address_flag as "Best Address Flag",
       city as "City",
       zipcode as "Zipcode",
       state as "State",
       employee_login as "Employee Login",
	   account_id,
	   customer_id,
	   day_id,
	   product_id,
	   position_id,
case when count(*) over (partition by interaction_id) > 1 then interaction_id else 0 end as fact_deduplication_factor
FROM :loadschema.ptab_f_interactions_call_curr f
)
WITH NO SCHEMA BINDING;
/* 
GRANT TRIGGER, RULE, SELECT, DELETE, UPDATE, REFERENCES, INSERT ON :loadschema.v_tab_Activity_Data_Calls TO oasis_cdw_tst_procuser;
GRANT SELECT ON :loadschema.v_tab_Activity_Data_Calls TO group oasis_cdw_tst_readuser_group;	


-- Check for Compile of VIEW
SELECT *
FROM :loadschema.v_tab_Activity_Data_Calls
LIMIT 1; */


DROP VIEW IF EXISTS :loadschema.v_tab_Activity_Data_Samples_Details;

CREATE OR REPLACE VIEW :loadschema.v_tab_Activity_Data_Samples_Details AS
(
SELECT CASE
   when datasource_id in ('OSS_CALL_DETAIL', 'VEEVA_CALL_DETAIL') AND call_type IN ('DETAIL ONLY', 'DETAIL WITH SAMPLE', 'GROUP DETAIL', 'GROUP DETAIL WITH SAMPLE')
     then  interaction_call_id                      
   END as Actual_Call,
       is_parent_call as "Is Parent Call",
       detail_priority as "Detail Priority",
       quantity,
	   f.territory_type as "Territory Type" ,
       f.week_ending_date as "Week Ending Date",
       f.day as "Day",
	   f.market as "Market",
	   f.therapeutic_class as "Therapeutic Class",
       f.therapeutic_sub_class as "Therapeutic Sub-Class",
	   CASE
   when datasource_id in ('OSS_CALL_SAMPLE', 'VEEVA_CALL_SAMPLE') AND call_type IN ('DETAIL WITH SAMPLE', 'GROUP DETAIL WITH SAMPLE', 'SAMPLE ONLY')
     then  quantity 
     else 0 
   END as "Sample Qty",
   CASE
   when datasource_id in ('OSS_CALL_DETAIL', 'VEEVA_CALL_DETAIL') AND call_type IN ('DETAIL ONLY', 'DETAIL WITH SAMPLE', 'GROUP DETAIL', 'GROUP DETAIL WITH SAMPLE')
     then  child_ld                      
   END as "Details",
       pre_call_notes as "Pre Call Notes",
       discussion_num_of_attendees as "Discussion # of Attendees",
       display_order as "Display Order",
       presentation_id,
       slide_version as "Slide Version",
       key_message_start_time as "Key Message Start Time",
       call_name as "Call Name",
       interaction_call_id as "Interaction Call Id",
       datasource_id,
       employee_id,
       address_id,
       attendee_type as "Attendee Type",
       call_type as "Call Type",
       interaction_sub_type as "Interaction Sub Type",
       interaction_type as "Interaction Type",
       detail_type as "Detail Type",
       delivery_status as "Delivery Status",
       child_type as "Child Type",
       product_type as "Product Type",
       discussion_topics as "Discussion Topics",
       product_strategy as "Product Strategy",
       clinical_research as "Clinical Research",
       discussion_class as "Discussion Class",
       device_trained as "Device Trained",
       call_focus as "Call Focus",
       category as "Category",
       reaction as "Reaction",
       activity_type as "Activity Type",
       call_status as "Call Status",
       street_address as "Street Address",
       best_address_flag as "Best Address Flag",
       city as "City",
       zipcode as "Zipcode",
       state as "State",
       employee_login as "Employee Login",
	   account_id,
	   customer_id,
	   day_id,
	   product_id,
	   position_id,
case when count(*) over (partition by interaction_id) > 1 then interaction_id else 0 end as fact_deduplication_factor
FROM :loadschema.ptab_f_interactions_call_curr f
WHERE datasource_id in ('OSS_CALL_SAMPLE', 'VEEVA_CALL_SAMPLE', 'OSS_CALL_DETAIL', 'VEEVA_CALL_DETAIL')


)
WITH NO SCHEMA BINDING;


/* GRANT TRIGGER, RULE, SELECT, DELETE, UPDATE, REFERENCES, INSERT ON :loadschema.v_tab_Activity_Data_Samples_Details TO oasis_cdw_tst_procuser;
GRANT SELECT ON :loadschema.v_tab_Activity_Data_Samples_Details TO group oasis_cdw_tst_readuser_group;


-- Check for Compile of VIEW
SELECT *
FROM :loadschema.v_tab_Activity_Data_Samples_Details
LIMIT 1;DROP VIEW IF EXISTS :loadschema.v_tab_Geography_Attributes; */

CREATE OR REPLACE VIEW :loadschema.v_tab_Geography_Attributes AS
(SELECT position_id,
        --Position 
   pos_salesforce_value as "Salesforce Value"       ,
--Any overlay positions are considered as separate territory types   
   case when pos_salesforce_value LIKE '%_O' THEN pos_salesforce_value else pos_type end as "Territory Type"                ,
   pos_type_flag as  "Active Territory Flag",
   --SalesForce Geography
   phi_top_hl_position_id as "Salesforce Geography Id"    ,
   phi_top_hl_position_name as "Salesforce Geography"       ,
   -- Area
   phi_hl8_position_id  as "Area Id"        ,
   phi_hl8_position_name   as "Area"     ,
   --Region
   phi_hl7_position_id    as "Region Id"      ,
   phi_hl7_position_name  as "Region"     ,
   --District
   phi_hl6_position_id  as "District Id"        ,
   phi_hl6_position_name  as  "District"      ,
   --POD
   phi_hl5_position_id   as "Pod Id"       ,
   phi_hl5_position_name   as "Pod"     ,
   --Territory
   phi_hl4_position_id  as "Territory Id"        ,
   phi_hl4_position_name    as "Territory"    ,
--Geo Archtype is used by the AOM business and is unique across POA periods   
    psg_archetype_flag as "Geo Archtype"
	   FROM :loadschema.ptab_d_position_full
where poa_active_flag = 'CURRENT' and phi_row_number < 2 
)
WITH NO SCHEMA BINDING;

/* GRANT TRIGGER, RULE, SELECT, DELETE, UPDATE, REFERENCES, INSERT ON :loadschema.v_tab_Geography_Attributes TO oasis_cdw_tst_procuser;
GRANT SELECT ON :loadschema.v_tab_Geography_Attributes TO group oasis_cdw_tst_readuser_group;

-- Check for Compile of VIEW
SELECT *
FROM :loadschema.v_tab_Geography_Attributes
LIMIT 1; */

DROP VIEW IF EXISTS :loadschema.v_tab_IC_Address_Attributes;
--This provides the IC address attributes joined to the fact data on customer, poa, and position within the Tableau datasource

CREATE OR REPLACE VIEW :loadschema.v_tab_IC_Address_Attributes AS
(
select  
city as "IC City",
coverage_eligible_flag as "Coverage Eligible Flag",
customer_id,
poa_id,
position_id,
shared_prescriber_flag as "Shared Prescriber Flag",
state as "IC State",
street_address as "IC Address",
target_flag as "IC Target Flag",
zip_code as "IC Zipcode"
from :sourcefacts.f_sales_customer_alignment

)
WITH NO SCHEMA BINDING;


/* GRANT TRIGGER, RULE, SELECT, DELETE, UPDATE, REFERENCES, INSERT ON :loadschema.v_tab_IC_Address_Attributes  TO oasis_cdw_tst_procuser;
GRANT SELECT ON :loadschema.v_tab_IC_Address_Attributes to group oasis_cdw_tst_readuser_group;

-- Check for Compile of VIEW
SELECT *
FROM :loadschema.v_tab_IC_Address_Attributes
LIMIT 1; */


DROP VIEW IF EXISTS :loadschema.v_tab_Managed_Care_Plan_Attributes;

CREATE OR REPLACE VIEW :loadschema.v_tab_Managed_Care_Plan_Attributes AS
(
SELECT plan_id,
	pln_account as account,
	pln_account_sub_group as "Account Sub Group",
	pln_account_type as "Account Type",
	pln_bk_of_biz as "Book of Bus",
	pln_bk_of_biz_sub as "Book of Bus Sg",
	pln_cont_ind as "Customer Type",
	pln_director as  director  ,
	pln_exec_director as "Exec Director"   ,
	pln_model_type as  "IMS Model Type"  ,
	pln_nmc_category_code as  "NNI Channel" ,
	pln_payer_name as  "Payer Channel"  ,
	plh_nmc_level_0_id as  "Payer Plan Id"  ,
	plh_nmc_level_0_name as  "Payer Plan Name" ,
	plh_nmc_level_1_id as   "Payer Channel Id",
	plh_nmc_level_1_name as   "Payer Channel Name" ,
	plh_nmc_level_2_id as   "Controlling PBM Id",
	plh_nmc_level_2_name as   "Controlling PBM Name"
FROM :loadschema.ptab_d_plan_full
--Matching plan_id count and not duplicating
WHERE pln_row_number < 2
)
WITH NO SCHEMA BINDING;

/* GRANT TRIGGER, RULE, SELECT, DELETE, UPDATE, REFERENCES, INSERT ON :loadschema.v_tab_Managed_Care_Plan_Attributes TO oasis_cdw_tst_procuser;
GRANT SELECT ON :loadschema.v_tab_Managed_Care_Plan_Attributes TO group oasis_cdw_tst_readuser_group;


-- Check for Compile of VIEW
SELECT *
FROM :loadschema.v_tab_Managed_Care_Plan_Attributes
LIMIT 1; */

DROP VIEW IF EXISTS :loadschema.v_tab_Planned_Activity;

CREATE OR REPLACE VIEW :loadschema.v_tab_Planned_Activity AS
(
SELECT 
interaction_type,
case  when cp.interaction_type = 'PLANNED DETAIL' then cp.num_of_planned_interactions end as "# of Planned Details",
case  when cp.interaction_type = 'PLANNED DETAIL' and cp.product_priority = 'PRIMARY' then cp.num_of_planned_interactions end as "# of Planned Calls",
num_of_planned_interactions as "Planned Call",
Case when interaction_type ='PLANNED DETAIL' and product_priority = 'PRIMARY' and cp.num_of_planned_interactions > 0 and poa.type like '%ACTIVITY%' AND poa.active_flag = 'CURRENT'
then cp.customer_id end as "Planned Target",
cp.product_priority as "Planned Detailing Position",
position_id, 
day_id, 
product_id, 
customer_id
FROM :sourcefacts.f_interactions_call_plan cp left join :sourcedims.d_poa poa on poa.poa_id = cp.poa_id
 )
WITH NO SCHEMA BINDING;

/* GRANT TRIGGER, RULE, SELECT, DELETE, UPDATE, REFERENCES, INSERT ON :loadschema.v_tab_Planned_Activity TO oasis_cdw_tst_procuser;
GRANT SELECT ON :loadschema.v_tab_Planned_Activity TO group oasis_cdw_tst_readuser_group;	


-- Check for Compile of VIEW
SELECT *
FROM :loadschema.v_tab_Planned_Activity
LIMIT 1;DROP VIEW IF EXISTS :loadschema.v_tab_Prescriber_Attributes; */

CREATE OR REPLACE VIEW :loadschema.v_tab_Prescriber_Attributes AS
(
SELECT customer_id,
   cus_designation_name as Title,
   cus_first_name as "First Name",
   cus_last_name as "Last Name",
   cus_full_name as "Full Name",
   cus_ims_id as "IMS Id",
   cus_current_source_unique_id as "Presriber Cdm Id",
   cus_no_contact_indicator as "AMA No Contact Flag",
   cus_restricted_rx_data_ind as "PDRP Flag",
   cus_me_number as "me #",
   cus_npi_id as "NPI Id",
   cus_pr_spec_code as "NNI Specialty Grouping",
   cus_pr_spec_group as "NNI Specialty Description",
   cus_pr_spec_name as Specialty,
   cus_professional_type as "Credential Type",
   cus_status as Status,
   cus_subtype as "CDM Subtype",
   cus_subtype as Credential,
   cus_type as "CDM Type",
   cus_best_addr_street as "Best Street Address",
   cus_best_addr_city as "Best City",
   cus_best_addr_state as "Best State",
   cus_best_addr_zipcode as "Best Zip",
   diabetes_target_flag as "Diabetes Target Flag",
   aom_target_flag as "AOM Target Flag"
FROM :loadschema.ptab_d_customer_full
--Matching customer_id count and not duplicating 
where cus_row_number < 2
)
WITH NO SCHEMA BINDING;

/* GRANT TRIGGER, RULE, SELECT, DELETE, UPDATE, REFERENCES, INSERT ON :loadschema.v_tab_Prescriber_Attributes TO oasis_cdw_tst_procuser;
GRANT SELECT ON :loadschema.v_tab_Prescriber_Attributes TO group oasis_cdw_tst_readuser_group;

-- Check for Compile of VIEW
SELECT *
FROM :loadschema.v_tab_Prescriber_Attributes
LIMIT 1;
DROP VIEW IF EXISTS :loadschema.v_tab_Prescriber_Attributes_with_All_Addresses; */

CREATE OR REPLACE VIEW :loadschema.v_tab_Prescriber_Attributes_with_All_Addresses AS
(
SELECT customer_id,
   cus_designation_name as Title,
   cus_first_name as "First Name",
   cus_last_name as "Last Name",
   cus_full_name as "Full Name",
   cus_ims_id as "IMS Id",
   cus_current_source_unique_id as "Prescriber Cdm Id",
   cus_no_contact_indicator as "AMA No Contact Flag",
   cus_restricted_rx_data_ind as "PDRP Flag",
   cus_me_number as "me #",
   cus_npi_id as "NPI Id",
   cus_pr_spec_code as "NNI Specialty Grouping",
   cus_pr_spec_group as "NNI Specialty Description",
   cus_pr_spec_name as Specialty,
   cus_professional_type as "Credential Type",
   cus_status as Status,
   cus_subtype as "CDM Subtype",
   cus_subtype as Credential,
   cus_type as "CDM Type",
   cus_best_addr_street as "Best Street Address",
   cus_best_addr_city as "Best City",
   cus_best_addr_state as "Best State",
   cus_best_addr_zipcode as "Best Zip",
   cad_addr_type_name as "Address Type",
	cad_address_id as address_id,
	cad_best_addr_indicator as "Best Address Indicator",
	cad_preferred_mailing_addr_indicator as "Preferred Mailing Address Indicator",
	cad_primary_addr as "Prescriber Street Address 1",
	cad_secondary_addr as "Prescriber Street Address 2",
	cad_city as "Prescriber City",
   cad_state as "Prescriber State",
   cad_country as "Prescriber Country",
   cad_zip as "Prescriber Zip",
   cad_addr_type_name as "Prescriber Address Type",
   cad_addr_status_code as "Prescriber Address Status",
	 diabetes_target_flag as "Diabetes Target Flag",
   aom_target_flag as "AOM Target Flag"
FROM :loadschema.ptab_d_customer_full
where cad_row_number < 2
)
WITH NO SCHEMA BINDING;

/* GRANT TRIGGER, RULE, SELECT, DELETE, UPDATE, REFERENCES, INSERT ON :loadschema.v_tab_Prescriber_Attributes_with_All_Addresses TO oasis_cdw_tst_procuser;
GRANT SELECT ON :loadschema.v_tab_Prescriber_Attributes_with_All_Addresses TO group oasis_cdw_tst_readuser_group;

-- Check for Compile of VIEW
SELECT *
FROM :loadschema.v_tab_Prescriber_Attributes_with_All_Addresses
LIMIT 1; */

DROP VIEW IF EXISTS :loadschema.v_tab_Prescriber_Attributes_with_Masking_Geography;

CREATE OR REPLACE VIEW :loadschema.v_tab_Prescriber_Attributes_with_Masking_Geography AS
(
SELECT 
   customer_id,
   cus_designation_name as Title,
   cus_first_name as "First Name",
   cus_last_name as "Last Name",
   cus_full_name as "Full Name",
   cus_ims_id as "IMS Id",
   cus_current_source_unique_id as "Prescriber CDM Id",
   cus_no_contact_indicator as "AMA No Contact Flag",
   cus_restricted_rx_data_ind as "PDRP Flag",
   cus_me_number as "me #",
   cus_npi_id as "NPI Id",
   cus_pr_spec_code as "NNI Specialty Grouping",
   cus_pr_spec_group as "NNI Specialty Description",
   cus_pr_spec_name as Specialty,
   cus_professional_type as "Credential Type",
   cus_status as Status,
   cus_subtype as "CDM Subtype",
   cus_subtype as Credential,
   cus_type as "Prescriber CDM Type",
   cus_best_addr_street as "Best Street Address",
   cus_best_addr_city as "Best City",
   cus_best_addr_state as "Best State",
   cus_best_addr_zipcode as "Best Zip",
   case when cm_masking_flag = 'M' then 'Y' else 'N' end as "Masked Prescriber Flag",
	cm_position_id as "Masked Prescriber Position",
 diabetes_target_flag as "Diabetes Target Flag",
   aom_target_flag as "AOM Target Flag"
FROM :loadschema.ptab_d_customer_full
where cm_row_number < 2
)
WITH NO SCHEMA BINDING;

/* GRANT TRIGGER, RULE, SELECT, DELETE, UPDATE, REFERENCES, INSERT ON :loadschema.v_tab_Prescriber_Attributes_with_Masking_Geography TO oasis_cdw_tst_procuser;
GRANT SELECT ON :loadschema.v_tab_Prescriber_Attributes_with_Masking_Geography TO group oasis_cdw_tst_readuser_group;

-- Check for Compile of VIEW
SELECT *
FROM :loadschema.v_tab_Prescriber_Attributes_with_Masking_Geography
LIMIT 1;
 */

DROP VIEW IF EXISTS :loadschema.v_tab_Prescriber_History_NBrx;

CREATE OR REPLACE VIEW :loadschema.v_tab_Prescriber_History_NBrx AS
(
SELECT 
       nbrx_unaligned_id
       poa_id,
       apportionment_nbrx as "Apportionment NBRx",
       apportionment_ltrx as "Apportionment LTRx",
       source_of_business as "Source of Business",
       brand_eligibility_flag as "Brand Eligibility Flag",
-- For performance on Redshift the most commonly and highly specific attributes have been added directly to the performance fact tables	   
  nbrx.market as "Market",
  nbrx.therapeutic_class as "Therapeutic Class",
 nbrx.therapeutic_sub_class as "Therapeutic Sub-Class",
 nbrx.territory_type as "Territory Type" ,
 nbrx.week_ending_date as "Week Ending Date",
 nbrx.day as "Day",
 product_id,
 from_product_id,
 customer_id,
 day_id,
 position_id,
 0 as overlay_deduplication_factor,
   0 as overlay_position_id
FROM :loadschema.ptab_f_sales_nbrx_terr nbrx
where    dynamics_type_id  <> 47

)
WITH NO SCHEMA BINDING;

/* GRANT TRIGGER, RULE, SELECT, DELETE, UPDATE, REFERENCES, INSERT ON :loadschema.v_tab_Prescriber_History_NBrx TO oasis_cdw_tst_procuser;
GRANT SELECT ON :loadschema.v_tab_Prescriber_History_NBrx TO group oasis_cdw_tst_readuser_group;		

-- Check for Compile of VIEW
SELECT *
FROM :loadschema.v_tab_Prescriber_History_NBrx
LIMIT 1; */


DROP VIEW IF EXISTS :loadschema.v_tab_Prescriber_History_NBrx_and_Continue;

CREATE OR REPLACE VIEW :loadschema.v_tab_Prescriber_History_NBrx_and_Continue AS
(
SELECT 
       nbrx_unaligned_id
       poa_id,
       apportionment_nbrx as "Apportionment NBRx",
       apportionment_ltrx as "Apportionment LTRx",
       source_of_business as "Source of Business",
       brand_eligibility_flag as "Brand Eligibility Flag",
-- For performance on Redshift the most commonly and highly specific attributes have been added directly to the performance fact tables	   
  nbrx.market as "Market",
  nbrx.therapeutic_class as "Therapeutic Class",
 nbrx.therapeutic_sub_class as "Therapeutic Sub-Class",
 nbrx.territory_type as "Territory Type" ,
 nbrx.week_ending_date as "Week Ending Date",
 nbrx.day as "Day",
 product_id,
 from_product_id,
 customer_id,
 day_id,
 position_id,
 0 as overlay_deduplication_factor,
 0 as overlay_position_id
FROM :loadschema.ptab_f_sales_nbrx_terr nbrx
)
WITH NO SCHEMA BINDING;


/* GRANT TRIGGER, RULE, SELECT, DELETE, UPDATE, REFERENCES, INSERT ON :loadschema.v_tab_Prescriber_History_NBrx_and_Continue TO oasis_cdw_tst_procuser;
GRANT SELECT ON :loadschema.v_tab_Prescriber_History_NBrx_and_Continue TO group oasis_cdw_tst_readuser_group;

-- Check for Compile of VIEW
SELECT *
FROM :loadschema.v_tab_Prescriber_History_NBrx_and_Continue
LIMIT 1; */
		
		DROP VIEW IF EXISTS :loadschema.v_tab_Prescriber_History_XPTRx;

CREATE OR REPLACE VIEW :loadschema.v_tab_Prescriber_History_XPTRx AS
(
SELECT trx.xpt_trx_ins_niad_aob_id,
       trx.poa_id as "POA id",
       trx.apportionment_niad_trx AS "Apportioned TRx Volume",
       trx.apportionment_niad_nrx as "Apportioned NIAD NRx Volume",
       trx.apportionment_trx AS "Apportioned SU Volume",
       trx.apportionment_nrx as "Apportioned NRx Volume",
       trx.category as "Category",
       trx.zip as "TRx Zip",
       trx.brand_eligibility_flag as "Brand Eligibility Flag",
       trx.first_time_writer_flag as "First Time Writer Flag",
       trx.recurrent_writer_flag as "Recurrent Writer Flag",
       trx.recurrent_writer_alt_flag as "Recurrent Writer Alt Flag",
	   trx.plan_exclusion_flag as "Plan Exclusion Flag",
	   -- For performance on Redshift the most commonly and highly specific attributes have been added directly to the performance fact tables	
	   trx.territory_type as "Territory Type" ,
       trx.week_ending_date as "Week Ending Date",
       trx.day as "Day",
	   trx.market as "Market",
	   trx.therapeutic_class as "Therapeutic Class",
       trx.therapeutic_sub_class as "Therapeutic Sub-Class",
	   customer_id,
       position_id,
	  day_id,
	   product_id,
	   plan_id   ,
	  0 as overlay_deduplication_factor,
          0 as overlay_position_id
FROM :loadschema.ptab_f_sales_xptrx_terr trx

)
WITH NO SCHEMA BINDING;

/* GRANT TRIGGER, RULE, SELECT, DELETE, UPDATE, REFERENCES, INSERT ON :loadschema.v_tab_Prescriber_History_XPTRx TO oasis_cdw_tst_procuser;
GRANT SELECT ON :loadschema.v_tab_Prescriber_History_XPTRx TO group oasis_cdw_tst_readuser_group;	

-- Check for Compile of VIEW
SELECT *
FROM :loadschema.v_tab_Prescriber_History_XPTRx
LIMIT 1;
DROP VIEW IF EXISTS :loadschema.v_tab_Product_Agile_Launch ; */

CREATE OR REPLACE VIEW :loadschema.v_tab_Product_Agile_Launch AS
(
select 
prodstat.product_id,
prodstat.product_name as "Launch Product",
prodstat.position_id,
prodstat.sales_area_type_code as "Sales Area Type",
prodstat.status,
prodstat.effective_date as "Effective Date",
prodstat.end_date as "End Date"
from :sourcedims.d_position_prod_agile_stat prodstat
)
WITH NO SCHEMA BINDING;


/* GRANT TRIGGER, RULE, SELECT, DELETE, UPDATE, REFERENCES, INSERT ON :loadschema.v_tab_Product_Agile_Launch  TO oasis_cdw_tst_procuser;
GRANT SELECT ON :loadschema.v_tab_Product_Agile_Launch TO group oasis_cdw_tst_readuser_group;



-- Check for Compile of VIEW
SELECT *
FROM :loadschema.v_tab_Product_Agile_Launch
LIMIT 1;DROP VIEW IF EXISTS :loadschema.v_tab_Product_Attributes; */

CREATE OR REPLACE VIEW :loadschema.v_tab_Product_Attributes AS
(
SELECT product_id,
        ---Products
  prd_chemical_entiry_description as  "Chemical Name",
  prd_device_code_description as Device,
  prd_form_description as "Delivery Form",
  prd_generic_indicator as "Generic Indicator",
  prd_manufacturer_name as manufacturer,
  nvl(prd_molecule_name, 
  case when prh_hierarchy_lvl < 7  
  then prh_hl6_product_name end) as "Molecule Name",
  --Market
  case when prh_hierarchy_lvl < 12
  then prh_top_hl_prod_name 
  end as Market,
  --Therapeutic_Category
  case when prh_hierarchy_lvl < 11
  then prh_h10_product_name 
  end as "Therapeutic Category",
  --Therapeutic_Class
  case when prh_hierarchy_lvl < 10
  then prh_hl9_product_name 
  end as "Therapeutic Class",
  --Therapeutic_Sub_Class
  case when prh_hierarchy_lvl < 9
  then prh_hl8_product_name 
  end as "Therapeutic Sub-Class",
  --Product_Franchise
  case when  prh_hierarchy_lvl < 8
  then prh_hl7_product_name
  end  as "Product Franchise",
  --Brand
  case when prh_hierarchy_lvl < 7  
  then prh_hl6_product_name
  end   as Brand,
  --Form_Strength
  case when prh_hierarchy_lvl < 6 
  then prh_hl5_product_name
  end  as "Form Strength"
FROM :loadschema.ptab_d_product_full
-- Equivalent to Inner Joining on Product Hierarchy
where prh_row_number = 1
)
WITH NO SCHEMA BINDING;

/* GRANT TRIGGER, RULE, SELECT, DELETE, UPDATE, REFERENCES, INSERT ON :loadschema.v_tab_Product_Attributes TO oasis_cdw_tst_procuser;
GRANT SELECT ON :loadschema.v_tab_Product_Attributes TO group oasis_cdw_tst_readuser_group;

-- Check for Compile of VIEW
SELECT *
FROM :loadschema.v_tab_Product_Attributes
LIMIT 1; */


		
DROP VIEW IF EXISTS :loadschema.v_tab_SalesDDD_Measures_and_Attributes;

CREATE OR REPLACE VIEW :loadschema.v_tab_SalesDDD_Measures_and_Attributes AS
(

SELECT 
ddd_sub.channel as Channel,
ddd_sub.sub_cat_description as "Sub Cat Name",
sales_ddd.hierarchy_lvl,
case  when  row_number() over ( partition by  sales_ddd.ddd_unaligned_id order by sales_ddd.ddd_id )  = 1 then 1 else 0 end  * sales_ddd.s_amount as "DDD Amount",
case  when  row_number() over ( partition by  sales_ddd.ddd_unaligned_id order by sales_ddd.ddd_id )  = 1 then 1 else 0 end  * sales_ddd.s_units as "DDD Volume",
sales_ddd.ddd_unaligned_id,
sales_ddd.outlet_number as "DDD Id",
sales_ddd.outlet_name as "DDD Outlet",
sales_ddd.address as "DDD Address",
sales_ddd.city as "DDD City",
sales_ddd.state as "DDD State",
sales_ddd.zip as "DDD Zip",
sales_ddd.poa_id,
sales_ddd.product_id, 
sales_ddd.day_id, 
sales_ddd.position_id, 
acc.account_id
FROM :sourcefacts.f_sales_ddd sales_ddd
left join :sourcedims.d_ddd_subcat_grouping ddd_sub on ddd_sub.sub_cat_code  = sales_ddd.sub_category
 )
WITH NO SCHEMA BINDING;

/* GRANT TRIGGER, RULE, SELECT, DELETE, UPDATE, REFERENCES, INSERT ON :loadschema.v_tab_SalesDDD_Measures_and_Attributes TO oasis_cdw_tst_procuser;
GRANT SELECT ON :loadschema.v_tab_SalesDDD_Measures_and_Attributes TO  group oasis_cdw_tst_readuser_group;	

-- Check for Compile of VIEW
SELECT *
FROM :loadschema.v_tab_SalesDDD_Measures_and_Attributes
LIMIT 1; */

DROP VIEW IF EXISTS :loadschema.v_tab_Time_Period_Attributes;

CREATE OR REPLACE VIEW :loadschema.v_tab_Time_Period_Attributes AS
(
SELECT 	day_id,
        d_day_dt as "Day"                  ,
		d_split_week_ending_date  as "Week Ending Date"  ,
		d_poa as "Activity Poa Name Diabetes",
		d_poa_aom as "Activity Poa Name AOM",
		d_sales_poa as "Sales Poa Name Diabetes",
		d_sales_poa_aom as "Sales Poa Name AOM"
FROM :loadschema.ptab_d_day_full
)
WITH NO SCHEMA BINDING;

/* GRANT TRIGGER, RULE, SELECT, DELETE, UPDATE, REFERENCES, INSERT ON :loadschema.v_tab_Time_Period_Attributes TO oasis_cdw_tst_procuser;
GRANT SELECT ON :loadschema.v_tab_Time_Period_Attributes TO group oasis_cdw_tst_readuser_group;

-- Check for Compile of VIEW
SELECT *
FROM :loadschema.v_tab_Time_Period_Attributes
LIMIT 1; */

DROP VIEW IF EXISTS :loadschema.v_tab_Geography_Attributes_All_POAs;
--Created this version of the Geography view so that Activity Data, which is unaligned, could still be exposed within the latest position hierarchy
-- OBIEE excludes this data currently but Tableau users can the Retired Position Indicator to exclude or include this data

CREATE OR REPLACE VIEW :loadschema.v_tab_Geography_Attributes_All_POAs AS
(
--Using a Nest Select so that the latest position indicator can first be calculated and then used to display the last position hierarchy information for retired position ids
SELECT 
position_id,
"Salesforce Value",
"Territory Type",
"Active Territory Flag",
"Salesforce Geography Id" ,
"Salesforce Geography",
"Area Id",
"Area",
"Region Id" ,
"Region",
"District Id",
"District",
"Pod Id",
"Pod",
"Territory Id",
"Territory",
"POA Type",
"POA Name",
 poa_id,
 "Retired Position Indicator",
 "POA Start Day",
 "POA End Date",
 "Geo Archtype"
FROM
(SELECT position_id,
        --Position 
   pos_salesforce_value as "Salesforce Value"       ,
   case when pos_salesforce_value LIKE '%_O' THEN pos_salesforce_value else pos_type end as "Territory Type"                ,
   pos_type_flag as  "Active Territory Flag",
   --SalesForce Geography
   phi_top_hl_position_id as "Salesforce Geography Id"    ,
   phi_top_hl_position_name as "Salesforce Geography"       ,
   -- Area
   phi_hl8_position_id  as "Area Id"        ,
   phi_hl8_position_name   as "Area"     ,
   --Region
   phi_hl7_position_id    as "Region Id"      ,
   phi_hl7_position_name  as "Region"     ,
   --District
   phi_hl6_position_id  as "District Id"        ,
   phi_hl6_position_name  as  "District"      ,
   --POD
   phi_hl5_position_id   as "Pod Id"       ,
   phi_hl5_position_name   as "Pod"     ,
   --Territory
   phi_hl4_position_id  as "Territory Id"        ,
   phi_hl4_position_name    as "Territory"    ,
   --POA Information
   poa_type       as "POA Type"          ,
   poa_name    as "POA Name"            ,
   poa_poa_id as poa_id,
   case when
   row_number() over (PARTITION BY  position_id order by poa_start.day desc)
   = 1 then 'Yes' else 'No' end as "Latest_Position_Indicator",
   case when sum(case when poa_active_flag = 'CURRENT' then 1 else 0 end) over (partition by position_id) > 0
   then 'No' else 'Yes' end as "Retired Position Indicator",
   poa_start.day   as "POA Start Day"    ,
   poa_end.day     as "POA End Date"   ,
--Geo Archtype is used by the AOM business and is unique across POA periods   
   psg_archetype_flag as "Geo Archtype"
	   FROM :loadschema.ptab_d_position_full
--Providing the start and end dates of POA periods so users can see when a position was retired from alignment  
	   left join :loadschema.v_tab_Time_Period_Attributes poa_start on poa_start.day_id = poa_start_date_id
	   left join :loadschema.v_tab_Time_Period_Attributes poa_end on poa_end.day_id = poa_end_date_id
-- This ensures that no duplication is introduced from the employee bridge or employee data in the position_full table	   
where poa_row_number < 2
)
where "Latest_Position_Indicator" = 'Yes'
)
WITH NO SCHEMA BINDING;

/* GRANT TRIGGER, RULE, SELECT, DELETE, UPDATE, REFERENCES, INSERT ON :loadschema.v_tab_Geography_Attributes_All_POAs TO oasis_cdw_tst_procuser;
GRANT SELECT ON :loadschema.v_tab_Geography_Attributes_All_POAs TO group oasis_cdw_tst_readuser_group;

-- Check for Compile of VIEW
SELECT *
FROM :loadschema.v_tab_Geography_Attributes_All_POAs
LIMIT 1; */


DROP VIEW IF EXISTS :loadschema.v_tab_POA_Attributes;
--Created this version of the Geography view so that Activity Data, which is unaligned, could still be exposed within the latest position hierarchy
-- OBIEE excludes this data currently but Tableau users can the Retired Position Indicator to exclude or include this data

CREATE OR REPLACE VIEW :loadschema.v_tab_POA_Attributes AS
(
--Using a Nest Select so that the latest position indicator can first be calculated and then used to display the last position hierarchy information for retired position ids
SELECT DISTINCT
    poa_type       as "POA Type"          ,
   poa_name    as "POA Name"            ,
   poa_poa_id as poa_id,
   poa_num_of_months  as "POA Months" ,
   poa_active_flag as "POA Active Flag"        ,
   poa_prev_poa_name  as "Previous POA Name"  ,
    poa_start.day   as "POA Start Day"    ,
   poa_end.day     as "POA End Date"   
	   FROM :loadschema.ptab_d_position_full
--Providing the start and end dates of POA periods so users can see when a position was retired from alignment  
	   left join :loadschema.v_tab_Time_Period_Attributes poa_start on poa_start.day_id = poa_start_date_id
	   left join :loadschema.v_tab_Time_Period_Attributes poa_end on poa_end.day_id = poa_end_date_id
-- This ensures that no duplication is introduced from the employee bridge or employee data in the position_full table	   
where poa_row_number = 1

)
WITH NO SCHEMA BINDING;

/* GRANT TRIGGER, RULE, SELECT, DELETE, UPDATE, REFERENCES, INSERT ON :loadschema.v_tab_POA_Attributes TO oasis_cdw_tst_procuser;
GRANT SELECT ON :loadschema.v_tab_POA_Attributes TO group oasis_cdw_tst_readuser_group;

-- Check for Compile of VIEW
SELECT *
FROM :loadschema.v_tab_POA_Attributes
LIMIT 1; */

CREATE TABLE IF NOT EXISTS :loadschema.ptab_f_sales_nbrx_terr
(
   ptab_f_sales_nbrx_terr_id  bigint IDENTITY ENCODE ZSTD,
   customer_id           bigint  ENCODE ZSTD NOT NULL,
   product_id            integer  ENCODE ZSTD NOT NULL,
   day_id                integer  ENCODE ZSTD NOT NULL,
   dynamics_type_id      integer  ,
   position_id           integer  ENCODE ZSTD NOT NULL,
    from_product_id       integer  ENCODE ZSTD NOT NULL,
	territory_type            varchar(100) ,
   market                    varchar(100) ENCODE ZSTD ,
   therapeutic_class          varchar(100) ENCODE ZSTD,
   therapeutic_sub_class      varchar(100)  ENCODE ZSTD,
   week_ending_date           date         ENCODE ZSTD,
   "day"                      date         ENCODE ZSTD,
   nbrx_unaligned_id     varchar(255)  ENCODE ZSTD,
   poa_id                integer  ENCODE ZSTD,
   apportionment_nbrx    numeric(22,7)  ENCODE ZSTD,
   apportionment_ltrx    numeric(22,7)  ENCODE ZSTD,
 
   --Nbrx Dynamics Type Id from d_lov
   source_of_business		  varchar(50)  ENCODE ZSTD,
   --Brand Eligiblity Flag Yes/No
   brand_eligibility_flag  varchar(5)      ENCODE ZSTD,
   datasource_id           varchar(30)     ENCODE ZSTD,
   batch_id                       int     ENCODE ZSTD,
   source_unique_id        varchar(30)     ENCODE ZSTD
   
)
DISTKEY(ptab_f_sales_nbrx_terr_id)
COMPOUND SORTKEY (dynamics_type_id, territory_type, market, therapeutic_class, therapeutic_sub_class, week_ending_date, day, ptab_f_sales_nbrx_terr_id) 
;

CREATE TABLE IF NOT EXISTS :loadschema.ptab_f_interactions_call_curr
(
  interaction_id               bigint   ENCODE ZSTD,
   is_parent_call               varchar(1)   ENCODE ZSTD,
   detail_priority              numeric(2)   ENCODE ZSTD,
   quantity                     numeric(22,7)   ENCODE ZSTD,
   child_ld						varchar(255)    ENCODE ZSTD,
   pre_call_notes               varchar(255)   ENCODE ZSTD,
   discussion_num_of_attendees  numeric(22)   ENCODE ZSTD,
   clm_id                       varchar(100)   ENCODE ZSTD,
   display_order                numeric(22)   ENCODE ZSTD,
   presentation_id              varchar(100)   ENCODE ZSTD,
   slide_version                varchar(100)   ENCODE ZSTD,
   key_message_start_time       timestamp   ENCODE ZSTD,
   Call_Name                    varchar(80)   ENCODE ZSTD,
   interaction_call_id          varchar(255)   ENCODE ZSTD,
   datasource_id                varchar(30)  ,
   account_id                   bigint   ENCODE ZSTD NOT NULL,
   customer_id                  bigint  ENCODE ZSTD NOT NULL,
   product_id                   bigint   ENCODE ZSTD NOT NULL,
   employee_id                  bigint   ENCODE ZSTD ,
   position_id                  bigint   ENCODE ZSTD NOT NULL,
   day_id                       bigint   ENCODE ZSTD NOT NULL,
   address_id                   bigint   ENCODE ZSTD,
   isdeleted                    varchar(1)   ENCODE ZSTD,
   territory_type            varchar(100) ,
   market                    varchar(100) ENCODE ZSTD ,
   therapeutic_class          varchar(100) ENCODE ZSTD,
   therapeutic_sub_class      varchar(100)  ENCODE ZSTD,
   week_ending_date           date         ENCODE ZSTD,
   "day"                      date         ENCODE ZSTD,
   source_unique_id           varchar(255)  ENCODE ZSTD,
   --Add LOV tables for each LID
   attendee_type       varchar(100)   ENCODE ZSTD,	
   call_type              varchar(100)   ENCODE ZSTD,	
   interaction_sub_type     varchar(100)   ENCODE ZSTD,	
   interaction_type       varchar(100)   ENCODE ZSTD,	
   detail_type              varchar(100)   ENCODE ZSTD,	
   delivery_status          varchar(100)   ENCODE ZSTD,	
   child_type               varchar(100)   ENCODE ZSTD,	
   product_type             varchar(100)   ENCODE ZSTD,	
   discussion_topics        varchar(100)   ENCODE ZSTD,	
   product_strategy         varchar(100)   ENCODE ZSTD,	
   clinical_research        varchar(100)   ENCODE ZSTD,	
   discussion_class         varchar(100)   ENCODE ZSTD,	
   device_trained           varchar(100)   ENCODE ZSTD,	
   call_focus               varchar(100)   ENCODE ZSTD,	
   category                 varchar(100)   ENCODE ZSTD,	
   reaction                 varchar(100)   ENCODE ZSTD,	
   activity_type            varchar(100)   ENCODE ZSTD,	
   call_status              varchar(100)   ENCODE ZSTD,	
   --Address Attributes
    Street_Address			varchar(110)  ENCODE ZSTD,
	Best_Address_Flag		varchar(1)    ENCODE ZSTD,
	City					varchar(50)	  ENCODE ZSTD,
	Zipcode				    varchar(10)	  ENCODE ZSTD,
	State                   varchar(50)   ENCODE ZSTD,
	batch_id                       int     ENCODE ZSTD,
  -- Employee Number
    Employee_Login			varchar(5)	  ENCODE ZSTD
  
)	
COMPOUND SORTKEY (datasource_id, territory_type, market, therapeutic_class, therapeutic_sub_class, week_ending_date, day)   
;


CREATE TABLE IF NOT EXISTS :loadschema.ptab_d_product_full
(
   product_id                         bigint         ENCODE ZSTD NOT NULL,
   prd_product_name                   varchar(100)  ENCODE ZSTD,
   prd_type_code                      varchar(50)  ENCODE ZSTD,
   prd_chemical_name                  varchar(255)  ENCODE ZSTD,
   prd_strength                       varchar(255)  ENCODE ZSTD,
   prd_form_code                      varchar(16)  ENCODE ZSTD,
   prd_form_description               varchar(100)  ENCODE ZSTD,
   prd_units_per_case                 varchar(16)  ENCODE ZSTD,
   prd_manufacturer_name              varchar(255)  ENCODE ZSTD,
   prd_distributor_name               varchar(255)  ENCODE ZSTD,
   prd_volume                         varchar(48)  ENCODE ZSTD,
   prd_equivalent_unit                varchar(40)  ENCODE ZSTD,
   prd_chemical_entity_code           varchar(16)  ENCODE ZSTD,
   prd_chemical_entiry_description    varchar(100)  ENCODE ZSTD,
   prd_device_code                    varchar(16)  ENCODE ZSTD,
   prd_device_code_description        varchar(255)  ENCODE ZSTD,
   prd_therapy_code                   varchar(16)  ENCODE ZSTD,
   prd_therapy_code_description       varchar(18)  ENCODE ZSTD,
   prd_generic_indicator              varchar(10)  ENCODE ZSTD,
   prd_unit_description               varchar(255)  ENCODE ZSTD,
   prd_launch_date                    date  ENCODE ZSTD,
   prd_effective_date                 date  ENCODE ZSTD,
   prd_end_date                       date  ENCODE ZSTD,
   prd_status                         varchar(10)  ENCODE ZSTD,
   prd_molecule_name                  varchar(255)  ENCODE ZSTD,
   prd_product_form_code              varchar(256)  ENCODE ZSTD,
   prd_product_form_code_description  varchar(256)  ENCODE ZSTD,
   prd_regimen                        varchar(256)  ENCODE ZSTD,
   prd_product_description            varchar(256)  ENCODE ZSTD,
   prd_parent_product_id              varchar(20)  ENCODE ZSTD,
   prh_product_hierarchy_id  bigint      ENCODE ZSTD,
   prh_source_unique_id      varchar(30)  ENCODE ZSTD,
   prh_product_id            bigint  ENCODE ZSTD,
   prh_hierarchy_lvl         integer  ENCODE ZSTD,
   prh_top_hl_prod_id        varchar(20)  ENCODE ZSTD,
   prh_top_hl_prod_name      varchar(100) ,
   prh_hl1_product_id        varchar(20)  ENCODE ZSTD,
   prh_hl1_product_name      varchar(100)  ENCODE ZSTD,
   prh_hl2_product_id        varchar(20)  ENCODE ZSTD,
   prh_hl2_product_name      varchar(100)  ENCODE ZSTD,
   prh_hl3_product_id        varchar(20)  ENCODE ZSTD,
   prh_hl3_product_name      varchar(100)  ENCODE ZSTD,
   prh_hl4_product_id        varchar(20)  ENCODE ZSTD,
   prh_hl4_product_name      varchar(100)  ENCODE ZSTD,
   prh_hl5_product_id        varchar(20)  ENCODE ZSTD,
   prh_hl5_product_name      varchar(100)  ENCODE ZSTD,
   prh_hl6_product_id        varchar(20)  ENCODE ZSTD,
   prh_hl6_product_name      varchar(100)  ENCODE ZSTD,
   prh_hl7_product_id        varchar(20)  ENCODE ZSTD,
   prh_hl7_product_name      varchar(100) ENCODE ZSTD ,
   prh_hl8_product_id        varchar(20)  ENCODE ZSTD,
   prh_hl8_product_name      varchar(100)  ENCODE ZSTD,
   prh_hl9_product_id        varchar(20)  ENCODE ZSTD,
   prh_hl9_product_name      varchar(100)  ENCODE ZSTD,
   prh_h10_product_id        varchar(20)  ENCODE ZSTD,
   prh_h10_product_name      varchar(100)  ENCODE ZSTD,
   prh_status                varchar(10)  ENCODE ZSTD,
   prh_segment               varchar(256)  ENCODE ZSTD,
   prd_row_number                 bigint   ENCODE ZSTD,
   batch_id                       int     ENCODE ZSTD,
   prh_row_number                  bigint   ENCODE ZSTD
  
   
)
DISTSTYLE ALL
COMPOUND SORTKEY (prh_top_hl_prod_name, prh_h10_product_name, prh_hl9_product_name, prh_hl8_product_name, prh_hl7_product_name, prh_hl6_product_name  );
--Loading Data Into Table In Sort Key Order


CREATE TABLE IF NOT EXISTS :loadschema.ptab_d_position_full
(
   position_id           	 integer   ENCODE ZSTD NOT NULL,
   pos_source_unique_id      varchar(30)  ENCODE ZSTD,
   pos_name                  varchar(50)  ENCODE ZSTD,
   pos_type                  varchar(100)  ,
   pos_parent_name           varchar(50)  ENCODE ZSTD,
   pos_type_flag             varchar(10)  ENCODE ZSTD,
   pos_effective_start_date  date  ENCODE ZSTD,
   pos_end_date              date  ENCODE ZSTD,
   pos_sales_force           varchar(256)  ENCODE ZSTD,
   pos_salesforce_value      varchar(256)  ENCODE ZSTD,
   pos_position_lvl          varchar(255)  ENCODE ZSTD,
   phi_position_hierarchy_id  bigint  ENCODE ZSTD,
   phi_source_unique_id       varchar(30)  ENCODE ZSTD,
   phi_hierarchy_lvl          varchar(255)  ENCODE ZSTD,
   phi_top_hl_position_id     varchar(50)  ENCODE ZSTD,
   phi_top_hl_position_name   varchar(50)  ENCODE ZSTD,
   phi_top_hl_position_type   varchar(50)  ENCODE ZSTD,
   phi_hl1_position_id        varchar(50)  ENCODE ZSTD,
   phi_hl1_position_name      varchar(50)  ENCODE ZSTD,
   phi_hl1_position_type      varchar(50)  ENCODE ZSTD,
   phi_hl2_position_id        varchar(50)  ENCODE ZSTD,
   phi_hl2_position_name      varchar(50)  ENCODE ZSTD,
   phi_hl2_position_type      varchar(50)  ENCODE ZSTD,
   phi_hl3_position_id        varchar(50)  ENCODE ZSTD,
   phi_hl3_position_name      varchar(50)  ENCODE ZSTD,
   phi_hl3_position_type      varchar(50)  ENCODE ZSTD,
   phi_hl4_position_id        varchar(50)  ENCODE ZSTD,
   phi_hl4_position_name      varchar(50)  ENCODE ZSTD,
   phi_hl4_position_type      varchar(50)  ENCODE ZSTD,
   phi_hl5_position_id        varchar(50)  ENCODE ZSTD,
   phi_hl5_position_name      varchar(50)  ENCODE ZSTD,
   phi_hl5_position_type      varchar(50)  ENCODE ZSTD,
   phi_hl6_position_id        varchar(50)  ENCODE ZSTD,
   phi_hl6_position_name      varchar(50)  ENCODE ZSTD,
   phi_hl6_position_type      varchar(50)  ENCODE ZSTD,
   phi_hl7_position_id        varchar(50)  ENCODE ZSTD,
   phi_hl7_position_name      varchar(50)  ENCODE ZSTD,
   phi_hl7_position_type      varchar(50)  ENCODE ZSTD,
   phi_hl8_position_id        varchar(50)  ENCODE ZSTD,
   phi_hl8_position_name      varchar(50)  ENCODE ZSTD,
   phi_hl8_position_type      varchar(50)  ENCODE ZSTD,
   phi_effective_start_date   timestamp  ENCODE ZSTD,
   phi_end_date               timestamp  ENCODE ZSTD,
   phi_poa_name               varchar(50)  ENCODE ZSTD,
   phi_sales_force            varchar(255)  ENCODE ZSTD,
   phi_pod_primary            varchar(10)  ENCODE ZSTD,
   poa_poa_id             	      bigint     ENCODE ZSTD,
   poa_source_unique_id    varchar(100)  ENCODE ZSTD,
   poa_type                varchar(100)  ENCODE ZSTD,
   poa_name                varchar(50)  ENCODE ZSTD,
   poa_start_date_id       integer  ENCODE ZSTD,
   poa_end_date_id         integer  ENCODE ZSTD,
   poa_prev_start_date_id  integer  ENCODE ZSTD,
   poa_prev_end_date_id    integer  ENCODE ZSTD,
   poa_num_of_months       integer  ENCODE ZSTD,
   poa_field_force         varchar(20)  ENCODE ZSTD,
   poa_active_flag         varchar(10)  ENCODE ZSTD,
   poa_prev_poa_name       varchar(50)  ENCODE ZSTD,
   psg_position_segment_id  integer  ENCODE ZSTD,
   psg_source_unique_id     varchar(100)  ENCODE ZSTD,
   psg_archetype_flag       varchar(50)  ENCODE ZSTD,
   eps_empl_pstn_id          bigint  ENCODE ZSTD,
   eps_source_unique_id      varchar(30)  ENCODE ZSTD,
   eps_effective_start_date  timestamp  ENCODE ZSTD,
   eps_end_date              timestamp  ENCODE ZSTD,
   eps_active_flg            char(1)  ENCODE ZSTD,
   emp_employee_id       bigint  ENCODE ZSTD,
   emp_source_unique_id  varchar(30)  ENCODE ZSTD,
   emp_emp_num           varchar(20)  ENCODE ZSTD,
   emp_emp_initials      varchar(5)  ENCODE ZSTD,
   emp_first_name        varchar(50)  ENCODE ZSTD,
   emp_middle_name       varchar(50)  ENCODE ZSTD,
   emp_last_name         varchar(50)  ENCODE ZSTD,
   emp_full_name         varchar(100)  ENCODE ZSTD,
   emp_addr_street       varchar(200)  ENCODE ZSTD,
   emp_addr_city         varchar(50)  ENCODE ZSTD,
   emp_addr_state        varchar(2)  ENCODE ZSTD,
   emp_addr_zip          varchar(10)  ENCODE ZSTD,
   emp_email             varchar(100)  ENCODE ZSTD,
   emp_title             varchar(100)  ENCODE ZSTD,
   emp_hire_date         date  ENCODE ZSTD,
   emp_termination_date  date  ENCODE ZSTD,
   emp_work_phone        varchar(15)  ENCODE ZSTD,
   emp_work_phone_ext    varchar(5)  ENCODE ZSTD,
   emp_active_status     varchar(30)   ENCODE ZSTD,
   pos_row_number	     bigint  ENCODE ZSTD,
    psg_row_number		 int ENCODE ZSTD,
   phi_row_number        int ENCODE ZSTD, 
   poa_row_number        int ENCODE ZSTD,
   eps_row_number        int ENCODE ZSTD,
   batch_id                       int     ENCODE ZSTD,
   emp_row_number        int ENCODE ZSTD
)
DISTSTYLE ALL
COMPOUND SORTKEY 
(   pos_type ,
eps_row_number,
	 poa_active_flag
    )
;


CREATE TABLE IF NOT EXISTS :loadschema.ptab_d_plan_full
(
   plan_id             bigint       ENCODE ZSTD  NOT NULL   ,
   pln_source_unique_id    varchar(100)  ENCODE ZSTD,
   pln_ins_plan_id         varchar(100)  ENCODE ZSTD,
   pln_ins_plan_type       varchar(50)  ENCODE ZSTD,
   pln_ins_plan_name       varchar(100)  ENCODE ZSTD,
   pln_payer_id            varchar(30)  ENCODE ZSTD,
   pln_payer_type          varchar(50)  ENCODE ZSTD,
   pln_payer_name          varchar(100)  ENCODE ZSTD,
   pln_pbm_id              varchar(30)  ENCODE ZSTD,
   pln_pbm_name            varchar(100)  ENCODE ZSTD,
   pln_status_code         char(1)  ENCODE ZSTD,
   pln_model_type          varchar(20)  ENCODE ZSTD,
   pln_nmc_category_code   varchar(50)  ,
   pln_city                varchar(50)  ENCODE ZSTD,
   pln_hq_state            char(2)  ENCODE ZSTD,
   pln_operating_state     char(2)  ENCODE ZSTD,
   pln_account_name        varchar(100)  ENCODE ZSTD,
   pln_account_sub_group   varchar(100)  ENCODE ZSTD,
   pln_cont_ind            varchar(25)  ENCODE ZSTD,
   pln_bk_of_biz           varchar(100)  ENCODE ZSTD,
   pln_bk_of_biz_sub       varchar(100)  ENCODE ZSTD,
   pln_category_code       varchar(50)  ENCODE ZSTD,
   pln_category_code_name  varchar(50)  ENCODE ZSTD,
   pln_ims_payer_name      varchar(100)  ENCODE ZSTD,
   pln_account             varchar(100)  ENCODE ZSTD,
   pln_account_type        varchar(100)  ENCODE ZSTD,
   pln_exec_director       varchar(100)  ENCODE ZSTD,
   pln_director            varchar(100)  ENCODE ZSTD,
   pln_effective_date      date  ENCODE ZSTD,
   pln_end_date            date  ENCODE ZSTD,
   pln_active_flag         varchar(10)  ENCODE ZSTD,
   plh_plan_hierarchy_id      integer           ENCODE ZSTD,
   plh_source_unique_id       varchar(30)  ENCODE ZSTD,
   plh_fixed_hierarchy_level  numeric(10)  ENCODE ZSTD,
   plh_nmc_level_0_id         varchar(30)  ENCODE ZSTD,
   plh_nmc_level_0_name       varchar(100)  ENCODE ZSTD,
   plh_nmc_level_1_id         varchar(30)  ENCODE ZSTD,
   plh_nmc_level_1_name       varchar(100)  ENCODE ZSTD,
   plh_nmc_level_2_id         varchar(30)  ENCODE ZSTD,
   plh_nmc_level_2_name       varchar(100)  ENCODE ZSTD,
   plh_nmc_level_3_id         varchar(30)  ENCODE ZSTD,
   plh_nmc_level_3_name       varchar(100)  ENCODE ZSTD,
   plh_nmc_level_4_id         varchar(30)  ENCODE ZSTD,
   plh_nmc_level_4_name       varchar(100)  ENCODE ZSTD,
   plh_payer_type_id          integer  ENCODE ZSTD,
   plh_controlling_pbm        varchar(100)  ENCODE ZSTD,
   plh_effective_date         date  ENCODE ZSTD,
   plh_end_date               date  ENCODE ZSTD,
   plh_active_flag            varchar(10)  ENCODE ZSTD,
   plx_plan_exclusion_id  bigint          ENCODE ZSTD,
   pln_row_number          bigint 		ENCODE ZSTD,
   plh_row_number		  int ENCODE ZSTD,
     batch_id                       int     ENCODE ZSTD,
   plx_row_number         int ENCODE ZSTD
)
DISTSTYLE ALL
COMPOUND SORTKEY (pln_nmc_category_code, plan_id)
;


CREATE TABLE IF NOT EXISTS :loadschema.ptab_d_day_full
(
   sw_split_week_id     bigint ENCODE ZSTD,
   sw_week_num          varchar(6)  ENCODE ZSTD,
   sw_num_of_days       numeric(1)  ENCODE ZSTD,
   sw_start_date        date  ENCODE ZSTD,
   sw_end_date          date  ENCODE ZSTD,
   sw_week_end_date     date  ENCODE ZSTD,
   sw_calendar_month    date  ENCODE ZSTD,
   sw_month_end_date    date  ENCODE ZSTD,
   sw_year_month        varchar(6)  ENCODE ZSTD,
    day_id                  integer NOT NULL,
   d_day_dt                  date  ENCODE ZSTD,
   d_cal_mnth                numeric(2)  ENCODE ZSTD,
   d_cal_qtr                 numeric(1)  ENCODE ZSTD,
   d_cal_week                numeric(2)  ENCODE ZSTD,
   d_cal_year                numeric(4)  ENCODE ZSTD,
   d_day_name                varchar(30)  ENCODE ZSTD,
   d_mnth_name               varchar(30)  ENCODE ZSTD,
   d_mnth_strt_cal_dt_num    numeric(10)  ENCODE ZSTD,
   d_mnth_end_cal_dt_num     numeric(10)  ENCODE ZSTD,
   d_day_of_mnth             numeric(2)  ENCODE ZSTD,
   d_day_of_week             numeric(1)  ENCODE ZSTD,
   d_day_of_year             numeric(3)  ENCODE ZSTD,
   d_cal_dt_year_mnth        varchar(50)  ENCODE ZSTD,
   d_cal_dt_year_qtr         varchar(50)  ENCODE ZSTD,
   d_cal_dt_year_week        varchar(50)  ENCODE ZSTD,
   d_cal_dt_year             varchar(50)  ENCODE ZSTD,
   d_week_ending_date        date  ENCODE ZSTD,
   d_day_ago_num             integer  ENCODE ZSTD,
   d_day_ago_dt              date  ENCODE ZSTD,
   d_week_ago_num            integer  ENCODE ZSTD,
   d_week_ago_dt             date  ENCODE ZSTD,
   d_mnth_ago_num            integer  ENCODE ZSTD,
   d_mnth_ago_dt             date  ENCODE ZSTD,
   d_qtr_ago_num             integer  ENCODE ZSTD,
   d_qtr_ago_dt              date  ENCODE ZSTD,
   d_year_ago_num            integer  ENCODE ZSTD,
   d_year_ago_dt             date  ENCODE ZSTD,
   d_year_ago_strt_num       integer  ENCODE ZSTD,
   d_year_ago_end_num        integer  ENCODE ZSTD,
   d_mnth_ago_end_num        integer  ENCODE ZSTD,
   d_year_curr_strt_num      integer  ENCODE ZSTD,
   d_holiday_flag            integer  ENCODE ZSTD,
   d_poa                     varchar(30)  ENCODE ZSTD,
   d_poa_aom                 varchar(30)  ENCODE ZSTD,
   d_sales_poa               varchar(30)  ENCODE ZSTD,
   d_sales_poa_aom           varchar(30)  ENCODE ZSTD,
   batch_id                       int     ENCODE ZSTD,
   d_split_week_ending_date  date    ENCODE ZSTD       
   )
DISTSTYLE ALL
COMPOUND SORTKEY (day_id)
;

CREATE TABLE IF NOT EXISTS :loadschema.ptab_d_customer_full
(
   customer_id                    		  bigint NOT NULL,
   cus_source_unique_id                   varchar(30)  ENCODE ZSTD,
   cus_effective_start_date               date  ENCODE ZSTD,
   cus_end_date                           date  ENCODE ZSTD,
   cus_status                             varchar(50)  ENCODE ZSTD,
   cus_first_name                         varchar(100)  ENCODE ZSTD,
   cus_middle_name                        varchar(100)  ENCODE ZSTD,
   cus_last_name                          varchar(100)  ENCODE ZSTD,
   cus_full_name                          varchar(100)  ENCODE ZSTD,
   cus_supertype                          varchar(100)  ENCODE ZSTD,
   cus_type                               varchar(50)  ENCODE ZSTD,
   cus_subtype                            varchar(100)  ENCODE ZSTD,
   cus_npi_id                             varchar(50)  ENCODE ZSTD,
   cus_ims_id                             varchar(19)  ENCODE ZSTD,
   cus_hce_id                             varchar(50)  ENCODE ZSTD,
   cus_pr_spec_name                       varchar(100)  ENCODE ZSTD,
   cus_designation                        varchar(100)  ENCODE ZSTD,
   cus_deceased_date                      timestamp  ENCODE ZSTD,
   cus_job_title                          varchar(100)  ENCODE ZSTD,
   cus_gender                             varchar(50)  ENCODE ZSTD,
   cus_company_target_flag                char(1)  ENCODE ZSTD,
   cus_best_addr_street                   varchar(200)  ENCODE ZSTD,
   cus_best_addr_city                     varchar(50)  ENCODE ZSTD,
   cus_best_addr_state                    varchar(50)  ENCODE ZSTD,
   cus_best_addr_zipcode                  varchar(30)  ENCODE ZSTD,
   cus_kol_flg                            char(1)  ENCODE ZSTD,
   cus_dea_number                         varchar(20)  ENCODE ZSTD,
   cus_me_number                          varchar(20)  ENCODE ZSTD,
   cus_ama_number                         varchar(20)  ENCODE ZSTD,
   cus_aoa_number                         varchar(20)  ENCODE ZSTD,
   cus_assmca_number                      varchar(20)  ENCODE ZSTD,
   cus_no_contact_indicator               char(1)  ENCODE ZSTD,
   cus_restricted_rx_data_ind             char(1)  ENCODE ZSTD,
   cus_restricted_rx_data_effective_date  date  ENCODE ZSTD,
   cus_current_source_unique_id           varchar(20)  ENCODE ZSTD,
   cus_designation_code                   varchar(50)  ENCODE ZSTD,
   cus_designation_name                   varchar(100)  ENCODE ZSTD,
   cus_degree_code                        varchar(50)  ENCODE ZSTD,
   cus_degree_name                        varchar(100)  ENCODE ZSTD,
   cus_pr_spec_code                       varchar(255)  ENCODE ZSTD,
   cus_pr_spec_group                      varchar(255)  ENCODE ZSTD,
   cus_professional_type                  varchar(255)  ENCODE ZSTD,
     diabetes_target_flag               varchar(1)    ENCODE ZSTD ,
   aom_target_flag               varchar(1)   ENCODE ZSTD  ,
   cad_cust_addr_id                      bigint ENCODE ZSTD,
   cad_address_id                        bigint ENCODE ZSTD,
   cad_primary_addr                      varchar(100) ENCODE ZSTD,
   cad_secondary_addr                    varchar(100) ENCODE ZSTD,
   cad_city                              varchar(50) ENCODE ZSTD,
   cad_state                             varchar(50) ENCODE ZSTD,
   cad_country                           varchar(50) ENCODE ZSTD,
   cad_zip                               varchar(10) ENCODE ZSTD,
   cad_addr_type_code                    varchar(50) ENCODE ZSTD,
   cad_addr_type_name                    varchar(100) ENCODE ZSTD,
   cad_addr_status_code                  varchar(50) ENCODE ZSTD,
   cad_addr_status_name                  varchar(100) ENCODE ZSTD,
   cad_addr_effective_start_date         date ENCODE ZSTD,
   cad_addr_end_date                     date ENCODE ZSTD,
   cad_best_addr_indicator               char(1) ENCODE ZSTD,
   cad_preferred_mailing_addr_indicator  char(1) ENCODE ZSTD,
   cad_addr_rank_number                  integer ENCODE ZSTD,
   cad_altrnt_addr_rank_number           integer ENCODE ZSTD,
   cm_position_id             bigint     ENCODE ZSTD,
   cm_masking_flag            varchar(1) ENCODE ZSTD,
   cus_row_number                   bigint ENCODE ZSTD,
   cm_row_number             bigint ENCODE ZSTD,
   batch_id                       int     ENCODE ZSTD,
   cad_row_number				  bigint ENCODE ZSTD
)
DISTSTYLE ALL
COMPOUND SORTKEY (customer_id)
;

CREATE TABLE IF NOT EXISTS :loadschema.ptab_d_account_full
(
   account_id                 bigint ENCODE ZSTD  NOT NULL ,
   acc_source_unique_id           varchar(30)  ENCODE ZSTD,
   acc_name                       varchar(200)  ENCODE ZSTD,
   acc_abbr_name                  varchar(100)  ENCODE ZSTD,
   acc_supertype_code             varchar(50)  ENCODE ZSTD,
   acc_supertype_name             varchar(100)  ENCODE ZSTD,
   acc_type_code                  varchar(50)  ENCODE ZSTD,
   acc_type_name                  varchar(100)  ENCODE ZSTD,
   acc_subtype_code               varchar(50)  ENCODE ZSTD,
   acc_subtype_name               varchar(100)  ENCODE ZSTD,
   acc_status                     varchar(100)  ENCODE ZSTD,
   acc_accnt_address1             varchar(255)  ENCODE ZSTD,
   acc_accnt_address2             varchar(255)  ENCODE ZSTD,
   acc_accnt_city                 varchar(255)  ENCODE ZSTD,
   acc_accnt_state                varchar(255)  ENCODE ZSTD,
   acc_accnt_zip                  varchar(10)  ENCODE ZSTD,
   acc_effective_start_date       date  ENCODE ZSTD,
   acc_end_date                   date  ENCODE ZSTD,
   acc_class_of_trade_code        varchar(50)  ENCODE ZSTD,
   acc_class_of_trade_name        varchar(100)  ENCODE ZSTD,
   acc_previous_source_unique_id  varchar(20)  ENCODE ZSTD,
   acc_true_level                 varchar(100)  ENCODE ZSTD,
   acc_hcos_id                    varchar(20)  ENCODE ZSTD,
   acc_calculated_true_level      varchar(20)  ENCODE ZSTD,
   ahi_account_hierarchy_id  bigint  ENCODE ZSTD,
   ahi_fixed_hier_level      varchar(200)  ENCODE ZSTD,
   ahi_lvl1anc_accnt_id      varchar(200)  ENCODE ZSTD,
   ahi_lvl1anc_accnt_name    varchar(200)  ENCODE ZSTD,
   ahi_lvl2anc_accnt_id      varchar(200)  ENCODE ZSTD,
   ahi_lvl2anc_accnt_name    varchar(200)  ENCODE ZSTD,
   ahi_lvl3anc_accnt_id      varchar(200)  ENCODE ZSTD,
   ahi_lvl3anc_accnt_name    varchar(200)  ENCODE ZSTD,
   ahi_lvl4anc_accnt_id      varchar(200)  ENCODE ZSTD,
   ahi_lvl4anc_accnt_name    varchar(200)  ENCODE ZSTD,
   ahi_lvl5anc_accnt_id      varchar(200)  ENCODE ZSTD,
   ahi_lvl5anc_accnt_name    varchar(200)  ENCODE ZSTD,
   ahi_lvl6anc_accnt_id      varchar(200)  ENCODE ZSTD,
   ahi_lvl6anc_accnt_name    varchar(200)  ENCODE ZSTD,
   ahi_lvl7anc_accnt_id      varchar(200)  ENCODE ZSTD,
   ahi_lvl7anc_accnt_name    varchar(200)  ENCODE ZSTD,
   ahi_lvl8anc_accnt_id      varchar(200)  ENCODE ZSTD,
   ahi_lvl8anc_accnt_name    varchar(200)  ENCODE ZSTD,
   ahi_top_lvl_accnt_id      varchar(200)  ENCODE ZSTD,
   ahi_top_lvl_accnt_name    varchar(200)  ENCODE ZSTD,
   ahi_active_flag           varchar(10)  ENCODE ZSTD,
   ahi_true_level            varchar(200)  ,
   aff_affiliation_rollup_id  bigint ENCODE ZSTD,
   aff_affil_account_cdm_id   varchar(200) ENCODE ZSTD,
   aff_affil_customer_cdm_id  varchar(200) ENCODE ZSTD,
   aff_customer_id             bigint ENCODE ZSTD,
   aff_level                  varchar(30) ENCODE ZSTD,
   aff_affil_type             varchar(30) ENCODE ZSTD,
   aff_idn_affiliation_flag   varchar(1) ENCODE ZSTD,
   acc_row_number          bigint ENCODE ZSTD,
   ahi_row_number		  bigint ENCODE ZSTD,
   aff_row_number         bigint ENCODE ZSTD,
   affiliation_deduplication_id bigint ENCODE ZSTD,
   batch_id                       int     ENCODE ZSTD,
   "Highest Affiliation Level"  varchar(30) ENCODE ZSTD
   
)
DISTSTYLE ALL
COMPOUND SORTKEY (aff_level, aff_row_number, ahi_top_lvl_accnt_id, ahi_lvl8anc_accnt_id, ahi_lvl7anc_accnt_id  );

CREATE TABLE IF NOT EXISTS :loadschema.ptab_f_sales_xptrx_terr
(
   ptab_f_sales_xptrx_terr_id  bigint IDENTITY ENCODE ZSTD,
   xpt_trx_ins_niad_aob_id  bigint ENCODE ZSTD,
   customer_id              integer ENCODE ZSTD NOT NULL,
   position_id              integer  ENCODE ZSTD NOT NULL,
   product_id               integer ENCODE ZSTD NOT NULL,
   day_id                   integer NOT NULL,
   poa_id                   integer ENCODE ZSTD,
   plan_id                  integer  NOT NULL,
   territory_type            varchar(100) ,
   market                    varchar(100) ENCODE ZSTD ,
   therapeutic_class          varchar(100) ENCODE ZSTD,
   therapeutic_sub_class      varchar(100)  ENCODE ZSTD,
   week_ending_date           date         ENCODE ZSTD,
   "day"                      date         ENCODE ZSTD,
   apportionment_niad_trx   numeric(22,7) ENCODE ZSTD,
   apportionment_niad_nrx   numeric(22,7) ENCODE ZSTD,
   apportionment_trx        numeric(22,7) ENCODE ZSTD,
   apportionment_nrx        numeric(22,7) ENCODE ZSTD,
   category                 integer ENCODE ZSTD,
   zip                      varchar(10) ENCODE ZSTD,
   --Flags
   brand_eligibility_flag   varchar(100) ENCODE ZSTD,
   first_time_writer_flag      varchar(100) ENCODE ZSTD,
   recurrent_writer_flag     varchar(100)   ENCODE ZSTD,
   recurrent_writer_alt_flag    varchar(100)  ENCODE ZSTD,
   plan_exclusion_flag			varchar (100)  ENCODE ZSTD,
    datasource_id           varchar(30)     ENCODE ZSTD,
	batch_id                       int     ENCODE ZSTD,
   source_unique_id        varchar(30)     ENCODE ZSTD
 
)
DISTKEY(ptab_f_sales_xptrx_terr_id)
COMPOUND SORTKEY (territory_type, market, therapeutic_class, therapeutic_sub_class, week_ending_date, day,  ptab_f_sales_xptrx_terr_id) 
;

GRANT SELECT ON ALL TABLES IN SCHEMA :loadschema TO :readonlyusers;
GRANT SELECT ON ALL TABLES IN SCHEMA :loadschema TO :readonlygroups;

--Unsetting Self from Future Loads

UPDATE rpt_apollo.apollo_control_table
set batch_id = :batchid, last_loaded_date = getdate(), include_in_load='N'
WHERE sql_file_name = :'sqlfilename';

--Updating Historical Control Table
--Adding load stats into historical table
INSERT INTO rpt_apollo.h_apollo_control_table
(  h_apollo_control_table_id   ,
   sql_file_name     ,
   load_sequence           ,
   include_in_load          ,  
   last_loaded_date          ,
   total_records_loaded  ,
   batch_id
)
(select
   apollo_control_table_id   ,
   sql_file_name     ,
   load_sequence           ,
   include_in_load          ,  
   last_loaded_date          ,
   total_records_loaded  ,
   batch_id
from rpt_apollo.apollo_control_table
WHERE sql_file_name =  :'sqlfilename'
);