--Creating de-normalized dimension table with all attributes for time
-- REM Add compression encoding
-- REM Add sort key & distribution 


--Adding Start Time
--Updating latest run stats to apollo control table
UPDATE rpt_apollo.apollo_control_table
set last_start_date = getdate() , status = 'Started'
WHERE sql_file_name = :'sqlfilename';




CREATE TABLE IF NOT EXISTS :loadschema.ptab_d_product_full_new
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
--REM Add Order By   
   INSERT INTO :loadschema.ptab_d_product_full_new
(
   product_id                                   ,
   prd_product_name                   ,
   prd_type_code                      ,
   prd_chemical_name                  ,
   prd_strength                       ,
   prd_form_code                      ,
   prd_form_description               ,
   prd_units_per_case                 ,
   prd_manufacturer_name              ,
   prd_distributor_name               ,
   prd_volume                         ,
   prd_equivalent_unit                ,
   prd_chemical_entity_code           ,
   prd_chemical_entiry_description    ,
   prd_device_code                    ,
   prd_device_code_description        ,
   prd_therapy_code                   ,
   prd_therapy_code_description       ,
   prd_generic_indicator              ,
   prd_unit_description               ,
   prd_launch_date                    ,
   prd_effective_date                 ,
   prd_end_date                       ,
   prd_status                         ,
   prd_molecule_name                  ,
   prd_product_form_code              ,
   prd_product_form_code_description  ,
   prd_regimen                        ,
   prd_product_description            ,
   prd_parent_product_id              ,
   prh_product_hierarchy_id      ,
   prh_source_unique_id      ,
   prh_hierarchy_lvl         ,
   prh_top_hl_prod_id        ,
   prh_top_hl_prod_name      ,
   prh_hl1_product_id        ,
   prh_hl1_product_name      ,
   prh_hl2_product_id        ,
   prh_hl2_product_name      ,
   prh_hl3_product_id        ,
   prh_hl3_product_name      ,
   prh_hl4_product_id        ,
   prh_hl4_product_name      ,
   prh_hl5_product_id        ,
   prh_hl5_product_name      ,
   prh_hl6_product_id        ,
   prh_hl6_product_name      ,
   prh_hl7_product_id        ,
   prh_hl7_product_name      ,
   prh_hl8_product_id        ,
   prh_hl8_product_name      ,
   prh_hl9_product_id        ,
   prh_hl9_product_name      ,
   prh_h10_product_id        ,
   prh_h10_product_name      ,
   prh_status                ,
   prh_segment               ,
   prd_row_number             ,
   batch_id,
   prh_row_number
)
(
--Flattened product table is only 6000 rows; recommend to distribute to all nodes
SELECT 

   prd.product_id                         ,
   prd.product_name                   ,
   prd.type_code                      ,
   prd.chemical_name                  ,
   prd.strength                       ,
   prd.form_code                      ,
   prd.form_description               ,
   prd.units_per_case                 ,
   prd.manufacturer_name              ,
   prd.distributor_name               ,
   prd.volume                         ,
   prd.equivalent_unit                ,
   prd.chemical_entity_code           ,
   prd.chemical_entiry_description    ,
   prd.device_code                    ,
   prd.device_code_description        ,
   prd.therapy_code                   ,
   prd.therapy_code_description       ,
   prd.generic_indicator              ,
   prd.unit_description               ,
   prd.launch_date                    ,
   prd.effective_date                 ,
   prd.end_date                       ,
   prd.status                         ,
   prd.molecule_name                  ,
   prd.product_form_code              ,
   prd.product_form_code_description  ,
   prd.regimen                        ,
   prd.product_description            ,
   prd.parent_product_id              ,
   prh.product_hierarchy_id      ,
   prh.source_unique_id      ,
   cast(prh.hierarchy_lvl   as integer)      ,
   prh.top_hl_prod_id        ,
   prh.top_hl_prod_name      ,
   prh.hl1_product_id        ,
   prh.hl1_product_name      ,
   prh.hl2_product_id        ,
   prh.hl2_product_name      ,
   prh.hl3_product_id        ,
   prh.hl3_product_name      ,
   prh.hl4_product_id        ,
   prh.hl4_product_name      ,
   prh.hl5_product_id        ,
   prh.hl5_product_name      ,
   prh.hl6_product_id        ,
   prh.hl6_product_name      ,
   prh.hl7_product_id        ,
   prh.hl7_product_name      ,
   prh.hl8_product_id        ,
   prh.hl8_product_name      ,
   prh.hl9_product_id        ,
   prh.hl9_product_name      ,
   prh.h10_product_id        ,
   prh.h10_product_name      ,
   prh.status                ,
   prh.segment               ,
  row_number () over (partition by  prd.product_id ORDER BY prh.product_hierarchy_id  ) as prd_row_number,
  :batchid,
   case when prh.product_id is null then 0 else row_number () over (PARTITION BY prd.product_id, prh.product_hierarchy_id ORDER BY prh.product_hierarchy_id ) end as prh_row_number

FROM
:sourcedims.d_product prd
LEFT JOIN
:sourcedims.d_product_hierarchy prh
ON prd.product_id=prh.product_id
);



VACUUM FULL :loadschema.ptab_d_product_full_new TO 99 PERCENT;
ANALYZE :loadschema.ptab_d_product_full_new;

GRANT TRIGGER, RULE, SELECT, DELETE, UPDATE, REFERENCES, INSERT ON :loadschema.ptab_d_product_full_new TO :etluser;
GRANT SELECT ON :loadschema.ptab_d_product_full_new TO :readonlyusers;
GRANT SELECT ON :loadschema.ptab_d_product_full_new TO :readonlygroups;

DROP TABLE IF EXISTS :loadschema.ptab_d_product_full_old;

ALTER TABLE :loadschema.ptab_d_product_full RENAME TO ptab_d_product_full_old;

ALTER TABLE :loadschema.ptab_d_product_full_new RENAME TO ptab_d_product_full;



--Updating latest run stats to apollo control table
UPDATE rpt_apollo.apollo_control_table
set status = 'Successfully Completed', batch_id = :batchid, last_loaded_date = getdate() ,total_records_loaded =  (SELECT count(*) from :loadschema.ptab_d_product_full)
WHERE sql_file_name = :'sqlfilename';
