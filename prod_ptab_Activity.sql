/*----------------------------------------------------------------------------------
SQL File	: ptab_Activity_12.sql
View Name	: v_tab_activity_comb 
----------------------------------------------------------------------------------*/

/*----------------------------------------------------------------------------------
Adding Start Time
Updating latest run stats to apollo control table
----------------------------------------------------------------------------------*/
UPDATE :loadschema.apollo_control_table
set last_start_date = getdate(), status = 'Started'
WHERE sql_file_name = :'sqlfilename';

/*----------------------------------------------------------------------------------
1. Create Table ptab_interactions_call
----------------------------------------------------------------------------------*/
drop table if exists :loadschema.ptab_interactions_call cascade;

create table :loadschema.ptab_interactions_call
(
datasource_id varchar(256),
customer_id integer,
position_id integer,
product_id integer,
day_id integer,
Actual_Detail numeric(22,7),
Actual_sample numeric(22,7),
detail_priority numeric(2),
call_type_lid integer
)
DISTKEY (customer_id);


/*----------------------------------------------------------------------------------
1. Insert Query for rpt_apollo.ptab_interactions_call
----------------------------------------------------------------------------------*/
INSERT INTO :loadschema.ptab_interactions_call
(
  datasource_id,
  customer_id,
  position_id,
  product_id,
  day_id,
  Actual_Detail,
  Actual_Sample,
  detail_priority,
  call_type_lid
)
SELECT 'CALL' as datasource_id,
       customer_id,
       position_id,
       product_id,
       day_id,      
       Actual_Detail,
       Actual_sample,
	   detail_priority,
	   call_type_lid
FROM (SELECT call.customer_id,
             call.position_id,
             call.brand_product_id AS product_id,
             call.day_id,
             SUM(CASE WHEN call.datasource_id IN ('VEEVA_CALL_SAMPLE') THEN call.quantity ELSE 0 END) AS Actual_Sample,
             COUNT(DISTINCT CASE WHEN call.datasource_id IN ('VEEVA_CALL_DETAIL') THEN call.child_ld END) AS Actual_Detail,
             COUNT(DISTINCT CASE WHEN call.datasource_id IN ('VEEVA_CALL_DETAIL') THEN call.interaction_call_id END) AS Actual_call,
			 call.detail_priority,
			 call.call_type_lid
      FROM (SELECT f.*,
                   p.product_id AS brand_product_id
            FROM :sourcefacts.f_interactions_call f,
                 :sourcedims.d_product_hierarchy ph,
                 :sourcedims.d_product p
            WHERE f.datasource_id IN ('VEEVA_CALL_SAMPLE','VEEVA_CALL_DETAIL')
            AND   f.isdeleted = 'N'
            AND   f.product_id = ph.product_id
            AND   ph.hl6_product_id = p.source_unique_id
            ) call
      GROUP BY call.customer_id,
               call.position_id,
               call.day_id,
               call.brand_product_id,
			   call.detail_priority,
			   call.call_type_lid);

ANALYZE :loadschema.ptab_interactions_call;

UPDATE :loadschema.apollo_control_table
set status = '[1] Insert completed in ptab_interactions_call', last_loaded_date = getdate() 
WHERE sql_file_name = :'sqlfilename';		


/*----------------------------------------------------------------------------------
2. DDL for view rpt_apollo.v_tab_activity_comb
----------------------------------------------------------------------------------*/

create or replace view :loadschema.v_tab_activity_comb
as
SELECT 
       pos.source_unique_id AS Territory_Id,
       cust.source_unique_id AS CDM_ID,
       prodh.hl6_product_id AS brand_ID,
       prodh.hl6_product_name AS Brand,
       cust.ims_id AS ims_id,
       day.week_ending_date AS week_ending_date,
       day.day_dt AS actual_date,
       fact.detail_priority AS detail_priority,
       pos.type AS team,
       lov.name AS call_type,
       fact.Actual_Detail AS actual_detail,
       fact.Actual_sample AS actual_sample
FROM (SELECT * FROM :loadschema.ptab_interactions_call) fact
  LEFT OUTER JOIN :sourcedims.d_customer cust ON fact.customer_id = cust.customer_id
  LEFT OUTER JOIN :sourcedims.d_day day ON fact.day_id = day.day_id
  LEFT OUTER JOIN :sourcedims.d_product_hierarchy prodh ON fact.product_id = prodh.product_id
    LEFT OUTER JOIN :sourcedims.d_position pos ON fact.position_id = pos.position_id
  LEFT OUTER JOIN :sourcedims.d_lov lov ON fact.call_type_lid = lov.lov_id
  with no schema binding;	  

UPDATE :loadschema.apollo_control_table
set status = '[2] View Created : v_tab_activity_comb', last_loaded_date = getdate()  
WHERE sql_file_name = :'sqlfilename';	  

/*----------------------------------------------------------------------------------
Updating latest run stats to apollo control table
----------------------------------------------------------------------------------*/
UPDATE :loadschema.apollo_control_table
set status = 'Successfully Completed' ,batch_id = :batchid, last_loaded_date = getdate() ,total_records_loaded =  (SELECT count(*) from :loadschema.v_tab_activity_comb)
WHERE sql_file_name = :'sqlfilename';
