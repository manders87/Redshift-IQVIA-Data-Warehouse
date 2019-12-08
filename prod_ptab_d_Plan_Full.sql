--Creating de-normalized dimension table with all attributes for time
-- REM Add compression encoding
-- REM Add sort key & distribution
--Flattened product table is only 51000 rows; recommend to distribute to all nodes


--Adding Start Time
--Updating latest run stats to apollo control table
UPDATE rpt_apollo.apollo_control_table
set last_start_date = getdate(), status = 'Started' 
WHERE sql_file_name = :'sqlfilename';

-- Sorted on the primary key



CREATE TABLE IF NOT EXISTS :loadschema.ptab_d_plan_full_new
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
--Loading Data Into Table In Sort Key Order
--REM Add Order By   
   INSERT INTO :loadschema.ptab_d_plan_full_new
(
 plan_id                      ,
   pln_source_unique_id    ,
   pln_ins_plan_id         ,
   pln_ins_plan_type       ,
   pln_ins_plan_name       ,
   pln_payer_id            ,
   pln_payer_type          ,
   pln_payer_name          ,
   pln_pbm_id              ,
   pln_pbm_name            ,
   pln_status_code         ,
   pln_model_type          ,
   pln_nmc_category_code   ,
   pln_city                ,
   pln_hq_state            ,
   pln_operating_state     ,
   pln_account_name        ,
   pln_account_sub_group   ,
   pln_cont_ind            ,
   pln_bk_of_biz           ,
   pln_bk_of_biz_sub       ,
   pln_category_code       ,
   pln_category_code_name  ,
   pln_ims_payer_name      ,
   pln_account             ,
   pln_account_type        ,
   pln_exec_director       ,
   pln_director            ,
   pln_effective_date      ,
   pln_end_date            ,
   pln_active_flag         ,
   plh_plan_hierarchy_id   ,
   plh_source_unique_id       ,
   plh_fixed_hierarchy_level  ,
   plh_nmc_level_0_id         ,
   plh_nmc_level_0_name       ,
   plh_nmc_level_1_id         ,
   plh_nmc_level_1_name       ,
   plh_nmc_level_2_id         ,
   plh_nmc_level_2_name       ,
   plh_nmc_level_3_id         ,
   plh_nmc_level_3_name       ,
   plh_nmc_level_4_id         ,
   plh_nmc_level_4_name       ,
   plh_payer_type_id          ,
   plh_controlling_pbm        ,
   plh_effective_date         ,
   plh_end_date               ,
   plh_active_flag            ,
   plx_plan_exclusion_id      ,
   pln_row_number          ,
   plh_row_number		  ,
   batch_id               ,
   plx_row_number           
)
(
--Flattened product table is only 51000 rows; recommend to distribute to all nodes
SELECT 

   pln.plan_id             ,
   pln.source_unique_id    ,
   pln.ins_plan_id         ,
   pln.ins_plan_type       ,
   pln.ins_plan_name       ,
   pln.payer_id            ,
   pln.payer_type          ,
   pln.payer_name          ,
   pln.pbm_id              ,
   pln.pbm_name            ,
   pln.status_code         ,
   pln.model_type          ,
   pln.nmc_category_code   ,
   pln.city                ,
   pln.hq_state            ,
   pln.operating_state     ,
   pln.account_name        ,
   pln.account_sub_group   ,
   pln.cont_ind            ,
   pln.bk_of_biz           ,
   pln.bk_of_biz_sub       ,
   pln.category_code       ,
   pln.category_code_name  ,
   pln.ims_payer_name      ,
   pln.account             ,
   pln.account_type        ,
   pln.exec_director       ,
   pln.director            ,
   pln.effective_date      ,
   pln.end_date            ,
   pln.active_flag         ,
   plh.plan_hierarchy_id   ,
   plh.source_unique_id       ,
   plh.fixed_hierarchy_level  ,
   plh.nmc_level_0_id         ,
   plh.nmc_level_0_name       ,
   plh.nmc_level_1_id         ,
   plh.nmc_level_1_name       ,
   plh.nmc_level_2_id         ,
   plh.nmc_level_2_name       ,
   plh.nmc_level_3_id         ,
   plh.nmc_level_3_name       ,
   plh.nmc_level_4_id         ,
   plh.nmc_level_4_name       ,
   plh.payer_type_id          ,
   plh.controlling_pbm        ,
   plh.effective_date         ,
   plh.end_date               ,
   plh.active_flag            ,
   plx.plan_exclusion_id      ,
   row_number () over (partition by pln.plan_id order by plh. plan_hierarchy_id, plx.plan_exclusion_id ) as pln_row_number		  ,
   case when plh.plan_id is null then 0 else row_number () over (partition by pln.plan_id, plh. plan_hierarchy_id order by plh. plan_hierarchy_id, plx.plan_exclusion_id ) end as plh_row_number		  ,
   :batchid,
   case when plx.plan_id is null then 0 else row_number () over  (partition by pln.plan_id, plx.plan_exclusion_id order by plh. plan_hierarchy_id, plx.plan_exclusion_id) end as plx_row_number   
FROM
:sourcedims.d_plan pln
LEFT JOIN :sourcedims.d_plan_hierarchy plh
ON pln.plan_id=plh.plan_id
LEFT JOIN
:sourcedims.d_plan_exclusion plx
ON pln.plan_id=plx.plan_id
);



VACUUM FULL :loadschema.ptab_d_plan_full_new TO 99 PERCENT;
ANALYZE :loadschema.ptab_d_plan_full_new;

GRANT TRIGGER, RULE, SELECT, DELETE, UPDATE, REFERENCES, INSERT ON :loadschema.ptab_d_plan_full_new TO :etluser;
GRANT SELECT ON :loadschema.ptab_d_plan_full_new TO :readonlyusers;
GRANT SELECT ON :loadschema.ptab_d_plan_full_new TO :readonlygroups;

DROP TABLE IF EXISTS :loadschema.ptab_d_plan_full_old;

ALTER TABLE :loadschema.ptab_d_plan_full RENAME TO ptab_d_plan_full_old;

ALTER TABLE :loadschema.ptab_d_plan_full_new RENAME TO ptab_d_plan_full;


--Updating latest run stats to apollo control table
UPDATE rpt_apollo.apollo_control_table
set status = 'Successfully Completed', batch_id = :batchid, last_loaded_date = getdate() ,total_records_loaded =  (SELECT count(*) from :loadschema.ptab_d_plan_full)
WHERE sql_file_name = :'sqlfilename';
