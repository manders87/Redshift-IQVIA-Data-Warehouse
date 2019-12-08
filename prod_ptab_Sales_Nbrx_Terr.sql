--Include all keys and fact attributes from f_sales_nbrx_terr
--Include most commonly used dimensional attributes with high cardinality (territory_type, therapeutic_class, therapeutic_sub_class)

--Creating fact table that includes commonly used dimensional attributes
-- REM Add compression encoding
-- REM Add sort key & distribution
-- All sort keys should be encoded raw and all other fields set to ZSTD encoding
-- Fact tables should be distributed even or keyed on the largest dimensional table commonly joined


--Adding Start Time
--Updating latest run stats to apollo control table
UPDATE rpt_apollo.apollo_control_table
set last_start_date = getdate(), status = 'Started' 
WHERE sql_file_name = :'sqlfilename';

DROP TABLE IF EXISTS :loadschema.ptab_f_sales_nbrx_terr_old;

CREATE TABLE IF NOT EXISTS :loadschema.ptab_f_sales_nbrx_terr_new
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
--Loading Data Into Table In Sort Key Order
--REM Add Order By   
   INSERT INTO :loadschema.ptab_f_sales_nbrx_terr_new
(      customer_id,
       product_id,
       day_id,
       dynamics_type_id,
       position_id,
	    from_product_id,
		territory_type,
       market,
       therapeutic_class,
       therapeutic_sub_class,
       week_ending_date,
       "day",
       nbrx_unaligned_id,
       poa_id,
       apportionment_nbrx,
       apportionment_ltrx,
	    source_of_business,
		brand_eligibility_flag,
        datasource_id,
		batch_id ,
		source_unique_id

)
  

(
--Recommend to use Even distribution or Key distribution on customer_id if customer_id is equally distributed over commonly used query domain
-- Nbrx for non-market access Diabetes
SELECT 
   nvl(n.customer_id, -99999)           ,
   nvl(n.product_id, -99999)             ,
   nvl(n.day_id, -99999)                 ,
      n.dynamics_type_id        ,
   nvl(fca.position_id, -99999)           ,
   nvl(n.from_product_id, -99999)         ,
    case when pos.salesforce_value LIKE '%_O' THEN pos.salesforce_value else pos.type end as territory_type ,
   case when prod.prh_hierarchy_lvl < 12
  then prod.prh_top_hl_prod_name 
  end as market,
  case when prod.prh_hierarchy_lvl < 10
  then prod.prh_hl9_product_name 
  end as therapeutic_class,
   case when prod.prh_hierarchy_lvl < 9
  then prod.prh_hl8_product_name 
  end as therapeutic_sub_class,
   day.d_split_week_ending_date,
   day.d_day_dt,
   n.nbrx_unaligned_id       ,
   cast(n.poa_id as integer)                  ,
   --Cast to 3 decimal places
   NVL(CAST(n.nbrx * fca.apportionment_factor AS numeric(22,7)),0)       ,
   NVL(CAST(n.ltrx * fca.apportionment_factor AS numeric (22,7)) ,0)     ,
   --Nbrx Dynamics Type Id
   nbrx_dynamics_lov.name		 ,
   --Brand Eligiblity Flag Yes/No
   brand_eligibility_lov.value  ,
   n.datasource_id,
   :batchid,
   n.source_unique_id

 FROM
  :sourcefacts.f_sales_nbrx_unaligned n
    INNER JOIN 
  (SELECT fca.customer_id, fca.position_id, fca.poa_id, fca.alignment_level, fca.sales_team, fca.alignment_name, fca.apportionment_factor, fca.datasource_id, fca.overlay_position_id, fca.zip
  from :sourcefacts.f_customer_alignment fca
  group by fca.customer_id, fca.position_id, fca.poa_id, fca.alignment_level, fca.sales_team, fca.alignment_name, fca.apportionment_factor, fca.datasource_id, fca.overlay_position_id, fca.zip)
  fca ON (fca.customer_id = n.customer_id)
  INNER JOIN :sourcedims.d_poa poa2 ON (poa2.poa_id = fca.poa_id)
  INNER JOIN :sourcedims.d_position pos ON (fca.position_id = pos.position_id)
  INNER JOIN :sourcedims.d_position_hierarchy phi ON (pos.position_id = phi.position_id)
  INNER JOIN :sourcedims.d_poa poa ON (phi.poa_id=poa.poa_id)
  INNER JOIN :loadschema.ptab_d_product_full prod on prod.product_id = n.product_id
  LEFT JOIN :loadschema.ptab_d_day_full day on day.day_id = n.day_id
  LEFT JOIN :sourcedims.d_lov nbrx_dynamics_lov ON (nbrx_dynamics_lov.lov_id = n.dynamics_type_id and nbrx_dynamics_lov.type='NBRX_DYNAMICS_TYPE')
  LEFT JOIN :sourcedims.d_lov brand_eligibility_lov on (brand_eligibility_lov.lov_id = n.brand_eligibility_id and brand_eligibility_lov.type ='YES_NO_FLAG')
  WHERE (poa.active_flag = 'CURRENT' and poa2.active_flag = 'CURRENT') and fca.alignment_level = 'TERRITORY' AND fca.datasource_id in ( 'PROF' ,'PROF_NBRX') AND UPPER(poa2.type) like '%SALES%'
  and phi.sales_force in ( 
  --Diabetes Data
  'HSDE','DIABETES CARE')
  and n.datasource_id = 'DIAB_NBRX'
UNION ALL
-- Nbrx for non-Market Access AOB
SELECT 
   nvl(n.customer_id, -99999)           ,
   nvl(n.product_id, -99999)             ,
   nvl(n.day_id, -99999)                 ,
      n.dynamics_type_id        ,
   nvl(fca.position_id, -99999)           ,
   nvl(n.from_product_id, -99999)         ,
    case when pos.salesforce_value LIKE '%_O' THEN pos.salesforce_value else pos.type end as territory_type ,
   case when prod.prh_hierarchy_lvl < 12
  then prod.prh_top_hl_prod_name 
  end as market,
  case when prod.prh_hierarchy_lvl < 10
  then prod.prh_hl9_product_name 
  end as therapeutic_class,
   case when prod.prh_hierarchy_lvl < 9
  then prod.prh_hl8_product_name 
  end as therapeutic_sub_class,
   day.d_split_week_ending_date,
   day.d_day_dt,
   n.nbrx_unaligned_id       ,
   cast(n.poa_id as integer)                  ,
   --Cast to 3 decimal places
   NVL(CAST(n.nbrx * fca.apportionment_factor AS numeric(22,7)),0)       ,
   NVL(CAST(n.ltrx * fca.apportionment_factor AS numeric (22,7)) ,0)     ,
   --Nbrx Dynamics Type Id
   nbrx_dynamics_lov.name		 ,
   --Brand Eligiblity Flag Yes/No
   brand_eligibility_lov.value  ,
    n.datasource_id,
	:batchid,
   n.source_unique_id
FROM
  
  :sourcefacts.f_sales_nbrx_unaligned n
    INNER JOIN 
  (SELECT fca.customer_id, fca.position_id, fca.poa_id, fca.alignment_level, fca.sales_team, fca.alignment_name, fca.apportionment_factor, fca.datasource_id, fca.overlay_position_id, fca.zip
  from :sourcefacts.f_customer_alignment fca
  group by fca.customer_id, fca.position_id, fca.poa_id, fca.alignment_level, fca.sales_team, fca.alignment_name, fca.apportionment_factor, fca.datasource_id, fca.overlay_position_id, fca.zip)
  fca ON (fca.customer_id = n.customer_id)
   INNER JOIN :sourcedims.d_poa poa2 ON (poa2.poa_id = fca.poa_id)
  INNER JOIN :sourcedims.d_position pos ON (fca.position_id = pos.position_id)
  INNER JOIN :sourcedims.d_position_hierarchy phi ON (pos.position_id = phi.position_id)
  INNER JOIN :sourcedims.d_poa poa ON (phi.poa_id=poa.poa_id)
  INNER JOIN :loadschema.ptab_d_product_full prod on prod.product_id = n.product_id
  INNER JOIN :loadschema.ptab_d_day_full day on day.day_id = n.day_id
  LEFT JOIN :sourcedims.d_lov nbrx_dynamics_lov ON (nbrx_dynamics_lov.lov_id = n.dynamics_type_id and nbrx_dynamics_lov.type='NBRX_DYNAMICS_TYPE')
  LEFT JOIN :sourcedims.d_lov brand_eligibility_lov on (brand_eligibility_lov.lov_id = n.brand_eligibility_id and brand_eligibility_lov.type ='YES_NO_FLAG')
  --Adding Target Flags  
  WHERE (poa.active_flag = 'CURRENT' and poa2.active_flag = 'CURRENT') and fca.alignment_level = 'TERRITORY' AND fca.datasource_id in ( 'PROF' ,'PROF_NBRX') AND UPPER(poa2.type) like '%SALES%'
  and phi.sales_force in ( 
  --AOB Data
  'AOM','OEP')
  and n.datasource_id = 'AOB_NBRX'
);
--Vacuuming and Analyzing to Speed Up Subsequent Market Access Load
VACUUM FULL  :loadschema.ptab_f_sales_nbrx_terr_new to 99 PERCENT;	
ANALYZE  :loadschema.ptab_f_sales_nbrx_terr_new ;	





	 

GRANT TRIGGER, RULE, SELECT, DELETE, UPDATE, REFERENCES, INSERT ON :loadschema.ptab_f_sales_nbrx_terr_new TO :etluser;
GRANT SELECT ON :loadschema.ptab_f_sales_nbrx_terr_new TO :readonlyusers;
GRANT SELECT ON :loadschema.ptab_f_sales_nbrx_terr_new TO :readonlygroups;	
		

--Creating a dummy entry so below doesn't fail if old table does not exist.
CREATE TABLE IF NOT EXISTS :loadschema.ptab_f_sales_nbrx_terr
(
   customer_id           bigint  ENCODE ZSTD NOT NULL
);


ALTER TABLE :loadschema.ptab_f_sales_nbrx_terr RENAME TO ptab_f_sales_nbrx_terr_old;

ALTER TABLE :loadschema.ptab_f_sales_nbrx_terr_new RENAME TO ptab_f_sales_nbrx_terr;


--Updating latest run stats to apollo control table
UPDATE rpt_apollo.apollo_control_table
set status = 'Successfully Completed', batch_id = :batchid, last_loaded_date = getdate() ,total_records_loaded =  (SELECT count(*) from :loadschema.ptab_f_sales_nbrx_terr)
WHERE sql_file_name = :'sqlfilename';

