DROP VIEW IF EXISTS rpt_apollo.v_tab_Customer_Alignment_MarketAccess;

CREATE VIEW rpt_apollo.v_tab_Customer_Alignment_MarketAccess AS
(
SELECT 
   customer_id             ,
   overlay_position_id      ,
   position_id              ,
   poa_id             ,
   plan_id,
   --De-duplications
   territory_type_deduplication_factor  ,
   Salesforce_Geography_deduplication_factor  ,
   Area_deduplication_factor  ,
   Region_deduplication_factor ,
   District_deduplication_factor ,
   Pod_deduplication_factor  ,
   Territory_deduplication_factor 
FROM rpt_apollo.ptab_f_Customer_Alignment fca
where fca.datasource_id in ('PROF_MKTACC') AND fca.sales_team ='RAM' 
UNION ALL
SELECT 
   customer_id             ,
   overlay_position_id      ,
   position_id              ,
   poa_id             ,
   plan_id,
   --De-duplications
   territory_type_deduplication_factor  ,
   Salesforce_Geography_deduplication_factor  ,
   Area_deduplication_factor  ,
   Region_deduplication_factor ,
   District_deduplication_factor ,
   Pod_deduplication_factor  ,
   Territory_deduplication_factor 
FROM rpt_apollo.ptab_f_Customer_Alignment fca
where fca.datasource_id in ('PBM_MANAM') AND fca.sales_team ='NAM'
)
WITH NO SCHEMA BINDING;

GRANT TRIGGER, RULE, SELECT, DELETE, UPDATE, REFERENCES, INSERT ON rpt_apollo.v_tab_Customer_Alignment_MarketAccess TO oasis_cdw_tst_procuser;
GRANT SELECT ON rpt_apollo.v_tab_Customer_Alignment_MarketAccess TO group oasis_cdw_tst_readuser_group;	

-- Check for Compile of VIEW
SELECT *
FROM rpt_apollo.v_tab_Customer_Alignment_MarketAccess
LIMIT 1;
