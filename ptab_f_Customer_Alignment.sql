DROP TABLE IF EXISTS rpt_apollo.ptab_f_Customer_Alignment_old;

CREATE TABLE IF NOT EXISTS rpt_apollo.ptab_f_Customer_Alignment_new
(
   sales_alignment_id       bigint,
   customer_id              integer ENCODE ZSTD NOT NULL,
   overlay_position_id      integer  ENCODE ZSTD NOT NULL,
   position_id              integer  ENCODE ZSTD NOT NULL,
   poa_id                   integer    ENCODE ZSTD NOT NULL,
   zip                      varchar(10) ENCODE ZSTD,
    datasource_id           varchar(30)     ENCODE ZSTD,
   alignment_name           varchar(50) ,
   alignment_level          varchar(50) ,
   sales_team               varchar(50) ENCODE ZSTD,
   apportionment_factor     numeric(22,7) ENCODE ZSTD,
   account_id               integer ENCODE ZSTD,
   target_flag              char(1) ENCODE ZSTD,
   plan_payer_pbm_id        varchar(20) ENCODE ZSTD,
   territory_type           varchar(100) ENCODE ZSTD,
   plan_id                  integer ENCODE ZSTD NOT NULL,

   --De-duplications
   territory_type_deduplication_factor int ENCODE ZSTD,
   Salesforce_Geography_deduplication_factor int ENCODE ZSTD,
   Area_deduplication_factor int ENCODE ZSTD,
   Region_deduplication_factor int ENCODE ZSTD,
   District_deduplication_factor int ENCODE ZSTD,
   Pod_deduplication_factor int ENCODE ZSTD,
   Territory_deduplication_factor int ENCODE ZSTD
   
 
)
DISTSTYLE ALL
COMPOUND SORTKEY (position_id, datasource_id, alignment_name, territory_type_deduplication_factor, Salesforce_Geography_deduplication_factor, 
   Area_deduplication_factor,
   Region_deduplication_factor,
   District_deduplication_factor,
   Pod_deduplication_factor,
   Territory_deduplication_factor) 
;


   INSERT INTO rpt_apollo.ptab_f_Customer_Alignment_new
(
    sales_alignment_id     ,
   customer_id             ,
   overlay_position_id     ,
   position_id             ,
   poa_id                  ,
   zip                     ,
    datasource_id          ,
   alignment_name           ,
   alignment_level           ,
   sales_team               ,
   apportionment_factor     ,
   account_id               ,
   target_flag             ,
   plan_payer_pbm_id        ,
   territory_type,
   plan_id,
   --De-duplications
   territory_type_deduplication_factor  ,
   Salesforce_Geography_deduplication_factor  ,
   Area_deduplication_factor  ,
   Region_deduplication_factor ,
   District_deduplication_factor ,
   Pod_deduplication_factor  ,
   Territory_deduplication_factor 
)

(
  SELECT 
   fca.sales_alignment_id     ,
   fca.customer_id             ,
   fca.overlay_position_id     ,
   fca.position_id             ,
   fca.poa_id                  ,
   fca.zip                     ,
   fca.datasource_id          ,
   fca.alignment_name           ,
   fca.alignment_level           ,
   fca.sales_team               ,
   fca.apportionment_factor     ,
   fca.account_id               ,
   fca.target_flag             ,
   fca.plan_payer_pbm_id        ,
   geo."Territory Type",
   -1,
row_number() over (partition by fca.customer_id, fca.overlay_position_id, geo."Territory Type" order by fca.position_id, fca.overlay_position_id) as territory_type_deduplication_factor,
  row_number() over (partition by fca.customer_id, fca.overlay_position_id,  geo."Salesforce Geography Id", geo."Territory Type" order by fca.position_id, fca.overlay_position_id)  as salesforce_geography_deduplication_factor,
    row_number() over (partition by fca.customer_id, fca.overlay_position_id, geo."Area Id", geo."Territory Type" order by fca.position_id, fca.overlay_position_id)   as area_deduplication_factor,
    row_number() over (partition by fca.customer_id, fca.overlay_position_id, geo."Region Id", geo."Territory Type" order by fca.position_id, fca.overlay_position_id)   as region_deduplication_factor,
      row_number() over (partition by fca.customer_id, fca.overlay_position_id,  geo."District Id", geo."Territory Type" order by fca.position_id, fca.overlay_position_id)   as district_deduplication_factor,
	  row_number() over (partition by fca.customer_id, fca.overlay_position_id,  geo."Pod Id", geo."Territory Type" order by fca.position_id, fca.overlay_position_id)  as pod_deduplication_factor,
	      row_number() over (partition by fca.customer_id, fca.overlay_position_id, geo."Territory Id", geo."Territory Type" order by fca.position_id, fca.overlay_position_id)  as territory_deduplication_factor
  from rpt_facts.f_customer_alignment fca
  inner join rpt_apollo.v_tab_Geography_Attributes geo on fca.position_id = geo.position_id
	INNER JOIN rpt_dims.d_poa poa2 ON (poa2.poa_id = fca.poa_id)  and poa2.active_flag = 'CURRENT' AND UPPER(poa2.type) like '%SALES%'
     where 
fca.alignment_level = 'TERRITORY' AND fca.datasource_id in ('PROF_MKTACC') AND fca.sales_team ='RAM' 
UNION ALL
SELECT 
   fca.sales_alignment_id     ,
   fca.customer_id             ,
   nvl(fca.overlay_position_id,-1)    ,
   fca.position_id             ,
   fca.poa_id                  ,
   fca.zip                     ,
   fca.datasource_id          ,
   fca.alignment_name           ,
   fca.alignment_level           ,
   fca.sales_team               ,
   fca.apportionment_factor     ,
   fca.account_id               ,
   fca.target_flag             ,
   fca.plan_payer_pbm_id        ,
  geo."Territory Type",
   plan.plan_id,
row_number() over (partition by plan.plan_id, geo."Territory Type" order by plan.plan_id) as territory_type_deduplication_factor,
  row_number() over (partition by plan.plan_id,  geo."Salesforce Geography Id", geo."Territory Type" order by plan.plan_id)  as salesforce_geography_deduplication_factor,
    row_number() over (partition by plan.plan_id, geo."Area Id", geo."Territory Type" order by plan.plan_id)   as area_deduplication_factor,
    row_number() over (partition by plan.plan_id, geo."Region Id", geo."Territory Type" order by plan.plan_id)   as region_deduplication_factor,
      row_number() over (partition by plan.plan_id,  geo."District Id", geo."Territory Type" order by plan.plan_id)   as district_deduplication_factor,
	  row_number() over (partition by plan.plan_id,  geo."Pod Id", geo."Territory Type" order by plan.plan_id)  as pod_deduplication_factor,
	      row_number() over (partition by plan.plan_id, geo."Territory Id", geo."Territory Type" order by plan.plan_id)  as territory_deduplication_factor
  from rpt_facts.f_customer_alignment fca
  inner join rpt_apollo.v_tab_Geography_Attributes geo on fca.position_id = geo.position_id
	INNER JOIN rpt_dims.d_poa poa2 ON (poa2.poa_id = fca.poa_id)  and poa2.active_flag = 'CURRENT' AND UPPER(poa2.type) like '%SALES%'
  LEFT join rpt_dims.d_plan_hierarchy plan on plan.nmc_level_2_id = fca.plan_payer_pbm_id
     where 
fca.alignment_level = 'TERRITORY' AND fca.datasource_id in ('PBM_MANAM') AND fca.sales_team ='NAM'
UNION ALL
SELECT 
   fca.sales_alignment_id     ,
   fca.customer_id             ,
   nvl(fca.overlay_position_id,-1)     ,
   fca.position_id             ,
   fca.poa_id                  ,
   fca.zip                     ,
   fca.datasource_id          ,
   fca.alignment_name           ,
   fca.alignment_level           ,
   fca.sales_team               ,
   fca.apportionment_factor     ,
   fca.account_id               ,
   fca.target_flag             ,
   fca.plan_payer_pbm_id        ,
  geo."Territory Type",
   -1,
   -1,
   -1,
   -1,
   -1,
   -1,
   -1,
   -1
  from rpt_facts.f_customer_alignment fca
  inner join rpt_apollo.v_tab_Geography_Attributes geo on fca.position_id = geo.position_id
	INNER JOIN rpt_dims.d_poa poa2 ON (poa2.poa_id = fca.poa_id)  and poa2.active_flag = 'CURRENT' AND UPPER(poa2.type) like '%SALES%'
     where 
fca.alignment_level = 'TERRITORY' AND fca.datasource_id NOT in ('PBM_MANAM') AND (fca.datasource_id not in ('PROF_MKTACC') AND fca.sales_team <>'RAM')
  )
  
;

VACUUM FULL  rpt_apollo.ptab_f_Customer_Alignment_new to 99 PERCENT;
ANALYZE rpt_apollo.ptab_f_Customer_Alignment_new; 



CREATE TABLE IF NOT EXISTS rpt_apollo.ptab_f_Customer_Alignment(
   customer_id           bigint  ENCODE ZSTD NOT NULL
);





ALTER TABLE rpt_apollo.ptab_f_Customer_Alignment RENAME TO ptab_f_Customer_Alignment_old;

ALTER TABLE rpt_apollo.ptab_f_Customer_Alignment_new RENAME TO ptab_f_Customer_Alignment;


GRANT SELECT ON rpt_apollo.ptab_f_Customer_Alignment TO group oasis_cdw_tst_readuser_group;
GRANT SELECT ON rpt_apollo.ptab_f_Customer_Alignment TO oasis_cdw_tst_tbl_read_user;




