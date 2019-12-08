/*----------------------------------------------------------------------------------
SQL File: 		ptab_Sales_Xptrx_Nbrx.sql
Table Name:		rpt_apollo.ptab_sales_xptrx_nbrx_combined
			rpt_apollo.ptab_sales_xptrx_nbrx_combined_alignment
View Name:		rpt_apollo.v_tab_sales_xptrx_nbrx_combined
			rpt_apollo.v_tab_sales_xptrx_nbrx_combined_alignment
----------------------------------------------------------------------------------*/

/*----------------------------------------------------------------------------------
Adding Start Time
Updating latest run stats to apollo control table
----------------------------------------------------------------------------------*/
UPDATE :loadschema.apollo_control_table
set last_start_date = getdate(), status = 'Started' 
WHERE sql_file_name = :'sqlfilename';

/*----------------------------------------------------------------------------------
1. Create Table ptab_sales_xptrx_nbrx_combined
----------------------------------------------------------------------------------*/
DROP TABLE IF EXISTS :loadschema.ptab_sales_xptrx_nbrx_combined cascade;

create table :loadschema.ptab_sales_xptrx_nbrx_combined
(
  customer_id                INTEGER,
  poa_id                     INTEGER,
  CDM_ID                     VARCHAR(30),
  IMS_ID                     VARCHAR(19),
  Week_End_Date              DATE,
  brand_id                   VARCHAR(20),
  brand_name                 VARCHAR(100),
  therapeutic_sub_class_id   VARCHAR(20),
  therapeutic_sub_class      VARCHAR(100),
  therapeutic_class_id       VARCHAR(20),
  therapeutic_class          VARCHAR(100),
  TRx                        NUMERIC(22,7),
  NBRx                       NUMERIC(22,7)  
)
DISTKEY (customer_id);

/*----------------------------------------------------------------------------------
1. Insert Query for rpt_apollo.ptab_sales_xptrx_nbrx_combined
----------------------------------------------------------------------------------*/
INSERT INTO :loadschema.ptab_sales_xptrx_nbrx_combined
(
  customer_id                ,
  poa_id                     ,
  CDM_ID                     ,
  IMS_ID                     ,
  Week_End_Date              ,
  brand_id                   ,
  brand_name                 ,
  therapeutic_sub_class_id   ,
  therapeutic_sub_class      ,
  therapeutic_class_id       ,
  therapeutic_class          ,
  TRx         ,
  NBRx        
)
SELECT fact.customer_id,
       fact.poa_id,
       cust.source_unique_id AS CDM_ID,
       cust.ims_id AS IMS_ID,
       day.split_week_ending_date AS Week_End_Date,
       prodh.hl6_product_id AS brand_id,
       prodh.hl6_product_name AS brand_name,
       prodh.hl8_product_id AS therapeutic_sub_class_id,
       prodh.hl8_product_name AS therapeutic_sub_class,
       prodh.hl9_product_id AS therapeutic_class_id,
       prodh.hl9_product_name AS therapeutic_class,
       fact.total_trx as trx,       
       fact.total_nbrx as nbrx
FROM (
SELECT COALESCE(cdwsales.customer_id,cdwnbrx.customer_id) AS customer_id,
       COALESCE(cdwsales.day_id,cdwnbrx.day_id) AS day_id,
       COALESCE(cdwsales.product_id,cdwnbrx.product_id) AS product_id,
       COALESCE(cdwsales.brand_id,cdwnbrx.brand_id) AS brand_id,
       COALESCE(cdwsales.poa_id,cdwnbrx.poa_id) AS poa_id,
       COALESCE(cdwsales.total_trx,0) AS Total_Trx,
       COALESCE(cdwnbrx.total_nbrx,0) AS Total_Nbrx
FROM (SELECT f.customer_id,
             f.product_id,
             f.day_id,
             f.brand_id,
             f.poa_id,
             SUM(f.total_trx) AS total_trx
      FROM :loadschema.ptab_sales_xptrx f
      GROUP BY f.customer_id,
               f.product_id,
               f.day_id,
               f.brand_id,
               f.poa_id) cdwsales
  FULL OUTER JOIN (SELECT f.customer_id,
                          f.product_id,
                          f.day_id,
                          f.brand_id,
                          f.poa_id,
                          SUM(f.total_nbrx) AS total_nbrx
                   FROM :loadschema.ptab_sales_nbrx f,
                        :sourcedims.d_lov l
                   WHERE l.lov_id = f.dynamics_type_id
                   AND   l.name NOT IN ('CONTINUE-5')
                   GROUP BY f.customer_id,
                            f.product_id,
                            f.day_id,
                            f.brand_id,
                            f.poa_id) cdwnbrx
               ON cdwsales.customer_id = cdwnbrx.customer_id
              AND cdwsales.product_id = cdwnbrx.product_id
              AND cdwsales.day_id = cdwnbrx.day_id
) fact
  LEFT OUTER JOIN :sourcedims.d_customer cust ON fact.customer_id = cust.customer_id
  LEFT OUTER JOIN :sourcedims.d_day day ON fact.day_id = day.day_id
  LEFT OUTER JOIN :sourcedims.d_product_hierarchy prodh ON fact.product_id = prodh.product_id;


ANALYZE :loadschema.ptab_sales_xptrx_nbrx_combined;

UPDATE :loadschema.apollo_control_table
set status = '[1] Insert completed in ptab_sales_xptrx_nbrx_combined' 
WHERE sql_file_name = :'sqlfilename';

/*----------------------------------------------------------------------------------
1. DDL for rpt_apollo.v_tab_sales_xptrx_nbrx_combined
----------------------------------------------------------------------------------*/
CREATE OR REPLACE VIEW :loadschema.v_tab_sales_xptrx_nbrx_combined
AS
select 
customer_id                ,
CDM_ID                     ,
IMS_ID                     ,
Week_End_Date              ,
brand_id                   ,
brand_name                 ,
therapeutic_sub_class_id   ,
therapeutic_sub_class      ,
therapeutic_class_id       ,
therapeutic_class          ,
TRx                        ,
NBRx 
from :loadschema.ptab_sales_xptrx_nbrx_combined
with no schema binding;

UPDATE :loadschema.apollo_control_table
set status = '[1] View Created : v_tab_sales_xptrx_nbrx_combined',last_loaded_date = getdate() 
WHERE sql_file_name = :'sqlfilename';

/*----------------------------------------------------------------------------------
2. Create Table ptab_sales_xptrx_nbrx_combined_alignment
----------------------------------------------------------------------------------*/
DROP TABLE IF EXISTS :loadschema.ptab_sales_xptrx_nbrx_combined_alignment cascade;

create table :loadschema.ptab_sales_xptrx_nbrx_combined_alignment
(
  customer_id                INTEGER,
  CDM_ID                     VARCHAR(30),
  IMS_ID                     VARCHAR(19),
  Week_End_Date              DATE,
  brand_id                   VARCHAR(20),
  brand_name                 VARCHAR(100),
  therapeutic_sub_class_id   VARCHAR(20),
  therapeutic_sub_class      VARCHAR(100),
  therapeutic_class_id       VARCHAR(20),
  therapeutic_class          VARCHAR(255),
  Territory_Id               VARCHAR(255),
  Territory                  VARCHAR(255),
  District_Id 	             VARCHAR(255),
  District	 	     VARCHAR(255),
  Region_Id 		     VARCHAR(255),
  Region	 	     VARCHAR(255),
  Area_Id	 	     VARCHAR(255),
  Area		 	     VARCHAR(255),
  pos_sales_team	     VARCHAR(255),
  salesforce_value	     VARCHAR(255),
  TRx         		     NUMERIC(22,7),
  NBRx      		     NUMERIC(22,7)  
)
DISTKEY (customer_id);
/*----------------------------------------------------------------------------------
2. Insert Query for rpt_apollo.ptab_sales_xptrx_nbrx_combined_alignment
----------------------------------------------------------------------------------*/
INSERT INTO :loadschema.ptab_sales_xptrx_nbrx_combined_alignment
(
customer_id                ,
CDM_ID                     ,
IMS_ID                     ,
Week_End_Date              ,
brand_id                   ,
brand_name                 ,
therapeutic_sub_class_id   ,
therapeutic_sub_class      ,
therapeutic_class_id       ,
therapeutic_class          ,
Territory_Id               ,
Territory                  ,
District_Id 		   ,
District	 	   ,
Region_Id 		   ,
Region	 		   ,
Area_Id	 		   ,
Area		 	   ,
pos_sales_team		   ,
salesforce_value	   ,
TRx         		   ,
NBRx  				   

)
SELECT fact.customer_id,
       fact.CDM_ID,
       fact.IMS_ID,
       fact.Week_End_Date,
       fact.brand_id,
       fact.brand_name,
       fact.therapeutic_sub_class_id,
       fact.therapeutic_sub_class,
       fact.therapeutic_class_id,
       fact.therapeutic_class,
       d_position_hierarchy.hl4_position_id AS Territory_Id,
       d_position_hierarchy.hl4_position_name AS Territory,
       d_position_hierarchy.hl6_position_id AS District_Id,
       d_position_hierarchy.hl6_position_name AS District,
       d_position_hierarchy.hl7_position_id AS Region_Id,
       d_position_hierarchy.hl7_position_name AS Region,
       d_position_hierarchy.hl8_position_id AS Area_Id,
       d_position_hierarchy.hl8_position_name AS Area,
	   pos.type as pos_sales_team,
	   pos.salesforce_value as salesforce_value,
       fact.total_trx AS trx,
       fact.total_nbrx AS nbrx
FROM (select 
unalign.customer_id,
unalign.cdm_id,
unalign.ims_id,
unalign.week_end_date,
unalign.brand_id,
unalign.brand_name,
unalign.therapeutic_sub_class_id,
unalign.therapeutic_sub_class,
unalign.therapeutic_class_id,
unalign.therapeutic_class,
unalign.poa_id,
coalesce(cust.position_id,0) as position_id,
unalign.trx*COALESCE(cust.apportionment_factor,1:: NUMERIC(22,7)):: NUMERIC(22,7) as Total_Trx ,
unalign.nbrx*COALESCE(cust.apportionment_factor,1:: NUMERIC(22,7)):: NUMERIC(22,7) as Total_Nbrx,
decode(cust.target_flag,'Y','Y','N') as target_flag
from 
(SELECT * from :loadschema.ptab_sales_xptrx_nbrx_combined_unaligned)unalign
left outer join 
 (select a.* from :sourcefacts.f_customer_alignment a inner join :sourcedims.d_poa poa
 on a.poa_id=poa.poa_id and poa.active_flag='CURRENT' AND   a.alignment_level = 'TERRITORY' AND a.datasource_id like 'PROF%'
 ) cust
 ON unalign.customer_id = cust.customer_id and unalign.poa_id = cust.poa_id ) fact
  LEFT OUTER JOIN (SELECT p.*
                   FROM :sourcedims.d_position_hierarchy p
                     INNER JOIN :sourcedims.d_poa poa
                             ON p.poa_id = poa.poa_id
                            AND poa.active_flag = 'CURRENT') d_position_hierarchy
               ON fact.position_id = d_position_hierarchy.position_id and fact.poa_id = d_position_hierarchy.poa_id			  
  LEFT OUTER JOIN :sourcedims.d_position pos ON fact.position_id = pos.position_id; 
  
ANALYZE :loadschema.ptab_sales_xptrx_nbrx_combined_alignment;

UPDATE :loadschema.apollo_control_table
set status = '[1] Insert completed in ptab_sales_xptrx_nbrx_combined_alignment' 
WHERE sql_file_name = :'sqlfilename';
  

/*----------------------------------------------------------------------------------
2. DDL for rpt_apollo.v_tab_sales_xptrx_nbrx_combined_alignment
----------------------------------------------------------------------------------*/
CREATE OR REPLACE VIEW :loadschema.v_tab_sales_xptrx_nbrx_combined_alignment
AS  
select 
customer_id                ,
CDM_ID                     ,
IMS_ID                     ,
Week_End_Date              ,
brand_id                   ,
brand_name                 ,
therapeutic_sub_class_id   ,
therapeutic_sub_class      ,
therapeutic_class_id       ,
therapeutic_class          ,
Territory_Id               ,
Territory                  ,
District_Id 			   ,
District	 			   ,
Region_Id 				   ,
Region	 				   ,
Area_Id	 				   ,
Area		 			   ,
pos_sales_team			   ,
salesforce_value		   ,
TRx         			   ,
NBRx  				   
from 
:loadschema.ptab_sales_xptrx_nbrx_combined_alignment
with no schema binding;  

UPDATE :loadschema.apollo_control_table
set status = '[2] View Created : v_tab_sales_xptrx_nbrx_combined_alignment',last_loaded_date = getdate() 
WHERE sql_file_name = :'sqlfilename';

/*----------------------------------------------------------------------------------
Updating latest run stats to apollo control table
----------------------------------------------------------------------------------*/
UPDATE :loadschema.apollo_control_table
set status = 'Successfully Completed', batch_id = :batchid, last_loaded_date = getdate() ,total_records_loaded =  (SELECT count(*) from :loadschema.v_tab_sales_xptrx_nbrx_combined)
WHERE sql_file_name = :'sqlfilename';  