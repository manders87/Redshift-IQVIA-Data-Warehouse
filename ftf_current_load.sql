--Creating temporary Xptr table optimizied to loading FTF
DROP TABLE IF EXISTS rpt_apollo.ptab_f_sales_xptrx_terr_ftf_temp;

CREATE TABLE IF NOT EXISTS rpt_apollo.ptab_f_sales_xptrx_terr_ftf_temp
(
   xpt_trx_ins_niad_aob_id  bigint ENCODE ZSTD,
   customer_id              integer ENCODE ZSTD NOT NULL,
   position_id              integer  ENCODE ZSTD NOT NULL,
   product_id               integer ENCODE ZSTD NOT NULL,
   day_id                   integer NOT NULL,
   poa_id                   integer ENCODE ZSTD,
   plan_id                  integer  NOT NULL,
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
   performance_key			    varchar (32)
)
DISTKEY (performance_key)
COMPOUND SORTKEY (performance_key) 
;

--Loading Data Into Table In Sort Key Order
--REM Add Order By   
   INSERT INTO rpt_apollo.ptab_f_sales_xptrx_terr_ftf_temp
(
   xpt_trx_ins_niad_aob_id,
       customer_id,
       position_id,
       product_id,
       day_id,
       poa_id,
       plan_id,
       apportionment_niad_trx,
       apportionment_niad_nrx,
       apportionment_trx,
       apportionment_nrx,
       category,
       zip,
       brand_eligibility_flag,
       first_time_writer_flag,
       recurrent_writer_flag,
       recurrent_writer_alt_flag,
	   plan_exclusion_flag,
	   performance_key
	   
  
)
  

(
--Recommend to use Even distribution or Key distribution on customer_id if customer_id is equally distributed over commonly used query domain
SELECT 
   xpt_trx_ins_niad_aob_id,
       customer_id,
       trx.position_id,
       product_id,
       trx.day_id,
       poa_id,
       plan_id,
       apportionment_niad_trx,
       apportionment_niad_nrx,
       apportionment_trx,
       apportionment_nrx,
       category,
       zip,
       brand_eligibility_flag,
       first_time_writer_flag,
       recurrent_writer_flag,
       recurrent_writer_alt_flag,
	   plan_exclusion_flag,
    MD5('P' + cast(plan_id as varchar(30)) + 'Pr' + cast(product_id as varchar(30))  + 'Z' + cast (zip as varchar(30)))

   
FROM
 rpt_apollo.ptab_f_sales_xptrx_terr trx
 inner join rpt_apollo.v_tab_Geography_Attributes geo on geo.position_id = trx.position_id
  --Using only the salesforce values in current xptrx_aligned
  and geo."Salesforce Value" in ('DCS1','HOUS_DCS','DCS1','EDC1','HOUS_EDCS','HSDCS1', 'HOUS_OCS','OCSR')
 inner join rpt_apollo.v_tab_Time_Period_Attributes time on time.day_id = trx.day_id
  where time."day" > (select dateadd(week,-20, max(snapshot_date)) from rpt_facts.fh_ftf_benefit_design)

  
);

VACUUM FULL  rpt_apollo.ptab_f_sales_xptrx_terr_ftf_temp to 99 PERCENT;
ANALYZE rpt_apollo.ptab_f_sales_xptrx_terr_ftf_temp;

GRANT TRIGGER, RULE, SELECT, DELETE, UPDATE, REFERENCES, INSERT ON rpt_apollo.ptab_f_sales_xptrx_terr_ftf_temp TO oasis_cdw_tst_procuser;
GRANT SELECT ON rpt_apollo.ptab_f_sales_xptrx_terr_ftf_temp TO group oasis_cdw_tst_readuser_group;

--Create temporary FTF table optimized for loading
DROP TABLE IF EXISTS rpt_apollo.ptab_fh_ftf_benefit_design_temp;


--Only contains key values, sort key values, and unique information from trx and ftf tables to minimize loading time.
CREATE TABLE IF NOT EXISTS rpt_apollo.ptab_fh_ftf_benefit_design_temp
(
   h_ftf_benefit_design_id  bigint     ENCODE ZSTD,
   source_unique_id         varchar(30)    ENCODE ZSTD,
   batch_id                 integer    ENCODE ZSTD,
   datasource_id            varchar(30)    ENCODE ZSTD,
   plan_id                  integer    ENCODE ZSTD,
   ftf_day_id                   integer    ENCODE ZSTD,
   product_id               integer   ENCODE ZSTD,
   ftf_plan_id              varchar(10)    ENCODE ZSTD,
   ftf_plan_name            varchar(100)    ENCODE ZSTD,
   ftf_provider_name        varchar(100)    ENCODE ZSTD,
   status                   varchar(100)    ENCODE ZSTD,
   material_restriction     char(1)    ENCODE ZSTD,
   class_nomenclature       varchar(100)    ENCODE ZSTD,
   subclass_name            varchar(100)    ENCODE ZSTD,
   subclass_nomenclature    varchar(100)    ENCODE ZSTD,
   restriction_detail       varchar(5000)    ENCODE ZSTD,
   copay_range              varchar(100)    ENCODE ZSTD,
   apportioned_weight       numeric(22,10)    ENCODE ZSTD,
   zip                      varchar(10)    ENCODE ZSTD,
   snapshot_date            date           ENCODE ZSTD,
-- Business Requirement that we only take the last 13 weeks of data relative to TRX
   "16_week_ago_day_id"       integer        ENCODE ZSTD,
   -- Distributed on concatenated hashed key to merge join with trx table
   performance_key          varchar(32)     
)
DISTKEY (performance_key)
COMPOUND SORTKEY (performance_key) 
;

INSERT INTO rpt_apollo.ptab_fh_ftf_benefit_design_temp
(
       h_ftf_benefit_design_id,
       source_unique_id,
       batch_id,
       datasource_id,
       plan_id,
       ftf_day_id,
       product_id,
       ftf_plan_id,
       ftf_plan_name,
       ftf_provider_name,
       status,
       material_restriction,
       class_nomenclature,
       subclass_name,
       subclass_nomenclature,
       restriction_detail,
       copay_range,
       apportioned_weight,
       zip,
       snapshot_date,
       "16_week_ago_day_id",
	   performance_key
)

(SELECT ftf.h_ftf_benefit_design_id,
       ftf.source_unique_id,
       ftf.batch_id,
       ftf.datasource_id,
       ftf.plan_id,
       ftf.day_id,
       ftf.product_id,
       ftf.ftf_plan_id,
       ftf.ftf_plan_name,
       ftf.ftf_provider_name,
       ftf.status,
       ftf.material_restriction,
       ftf.class_nomenclature,
       ftf.subclass_name,
       ftf.subclass_nomenclature,
       ftf.restriction_detail,
       ftf.copay_range,
       ftf.apportioned_weight,
       ftf.zip,
       ftf.snapshot_date,
       day.day_id,
       MD5('P' + cast(plan_id as varchar(30)) + 'Pr' + cast(product_id as varchar(30))  + 'Z' + cast (zip as varchar(30)))
       from rpt_facts.fh_ftf_benefit_design ftf inner join rpt_dims.d_day day on dateadd(day,-120,snapshot_date) = day.day_dt
	   where ftf.day_id = (select max(day_id) from rpt_facts.fh_ftf_benefit_design)
                
)
;

VACUUM FULL rpt_apollo.ptab_fh_ftf_benefit_design_temp to 99 PERCENT;

ANALYZE rpt_apollo.ptab_fh_ftf_benefit_design_temp;		 
		
GRANT TRIGGER, RULE, SELECT, DELETE, UPDATE, REFERENCES, INSERT ON rpt_apollo.ptab_fh_ftf_benefit_design_temp TO oasis_cdw_tst_procuser;
GRANT SELECT ON rpt_apollo.ptab_fh_ftf_benefit_design_temp TO group oasis_cdw_tst_readuser_group;


--Creating a temporary table with the needed distinct attributes from FTF optimized for loading
DROP TABLE IF EXISTS rpt_apollo.ptab_fh_ftf_benefit_design_distinct_temp;


--Only contains key values, sort key values, and unique information from trx and ftf tables to minimize loading time.
-- Will be joined to performance dimension tables in the schemantic view 
CREATE TABLE IF NOT EXISTS rpt_apollo.ptab_fh_ftf_benefit_design_distinct_temp
(  ftf_plan_id              varchar(10)  NOT NULL,
   ftf_plan_name            varchar(100)    ENCODE ZSTD,
   ftf_product_id           integer         ENCODE ZSTD,
    ftf_day_id                   integer    ENCODE ZSTD,
   status                   varchar(100)    ENCODE ZSTD,
   material_restriction     char(1)    ENCODE ZSTD,
   class_nomenclature       varchar(100)    ENCODE ZSTD,
   subclass_nomenclature    varchar(100)    ENCODE ZSTD,
   snapshot_date            date       NOT NULL,
  ftf_therapeutic_class      varchar(100)  ENCODE ZSTD,
  ftf_form_strength           varchar(100)  ENCODE ZSTD,
  ftf_performance_key         varchar(32)

)
DISTKEY (ftf_performance_key)
COMPOUND SORTKEY (ftf_performance_key, ftf_therapeutic_class) 
;

INSERT INTO rpt_apollo.ptab_fh_ftf_benefit_design_distinct_temp
(
      
       ftf_plan_id,
	   ftf_plan_name,
	   ftf_product_id,
	   ftf_day_id,
	   class_nomenclature, 
	   subclass_nomenclature,
	   material_restriction,
	   status,
	   snapshot_date,
	   ftf_therapeutic_class,
	   ftf_form_strength,
	   ftf_performance_key
	   
	   
  
)

(SELECT DISTINCT
           ftf.ftf_plan_id,
	   ftf.ftf_plan_name,
           ftf.product_id,
		   ftf.ftf_day_id,
	   ftf.class_nomenclature, 
	   ftf.subclass_nomenclature,
	   ftf.material_restriction,
	   ftf.status,
	   ftf.snapshot_date,
	   prod."Therapeutic Class",
	   prod."Form Strength",
	   md5('ftf'+cast (ftf_plan_id as varchar(15))+'d'+cast(ftf_day_id as varchar(15))) as ftf_performance_key
FROM rpt_apollo.ptab_fh_ftf_benefit_design_temp ftf
inner join rpt_apollo.v_tab_Product_Attributes prod on ftf.product_id = prod.product_id

)
;

VACUUM FULL rpt_apollo.ptab_fh_ftf_benefit_design_distinct_temp to 99 PERCENT;

ANALYZE rpt_apollo.ptab_fh_ftf_benefit_design_distinct_temp;		 
		
GRANT TRIGGER, RULE, SELECT, DELETE, UPDATE, REFERENCES, INSERT ON rpt_apollo.ptab_fh_ftf_benefit_design_distinct_temp TO oasis_cdw_tst_procuser;
GRANT SELECT ON rpt_apollo.ptab_fh_ftf_benefit_design_distinct_temp TO group oasis_cdw_tst_readuser_group;

-- Precalculates the join from ftf to trx and creates a temp table for the next process of joining to ftf distinct values
DROP TABLE IF EXISTS rpt_apollo.ptab_fh_ftf_benefit_design_loading_temp;
CREATE TABLE IF NOT EXISTS rpt_apollo.ptab_fh_ftf_benefit_design_loading_temp
(
   
   product_id               integer   ENCODE ZSTD NOT NULL,
   territory_type            varchar(100)  ENCODE ZSTD,
   ftf_plan_id              varchar(10)  NOT NULL,
   ftf_day_id                   integer    ENCODE ZSTD,
   day_id                   integer    ENCODE ZSTD NOT NULL,
   customer_id              bigint   ENCODE ZSTD NOT NULL,
   position_id              bigint   ENCODE ZSTD NOT NULL,
   nni_channel              varchar(50)  ENCODE ZSTD,
   status                   varchar(100)    ENCODE ZSTD,
   restriction_detail       varchar(5000)    ENCODE ZSTD,
   zip                      varchar(10)    ENCODE ZSTD,
    plan_id                   integer  NOT NULL     ,
    brand_eligibility_flag   varchar(100) ENCODE ZSTD,
   first_time_writer_flag      varchar(100) ENCODE ZSTD,
   recurrent_writer_flag     varchar(100)   ENCODE ZSTD,
   recurrent_writer_alt_flag    varchar(100)  ENCODE ZSTD,
   plan_exclusion_flag			varchar (100)  ENCODE ZSTD,
   snapshot_date            date     NOT NULL,
   ftf_therapeutic_class      varchar(100)  ENCODE ZSTD,
   ftf_performance_key        varchar(32) ,
   ftf_trx                  numeric(22,10)    ENCODE ZSTD
   


)
DISTKEY (ftf_performance_key)
COMPOUND SORTKEY (ftf_performance_key, ftf_therapeutic_class) 
;

INSERT INTO rpt_apollo.ptab_fh_ftf_benefit_design_loading_temp
(
       product_id,
	   territory_type,
       ftf_plan_id,
	   ftf_day_id,
	   day_id,
	   customer_id,
	   position_id,
	   nni_channel,
	   status,
	   restriction_detail,
       zip,
	   plan_id,
	   brand_eligibility_flag,
       first_time_writer_flag,
       recurrent_writer_flag,
       recurrent_writer_alt_flag,
	   plan_exclusion_flag,
       snapshot_date ,
	   ftf_therapeutic_class,
	   ftf_performance_key,
	   ftf_trx
	   
	   

)

(sELECT
       NVL(ftf.product_id , -99999),
	   geo."Territory Type" ,
       NVL(ftf.ftf_plan_id, '-99999'),
	   NVL(ftf.ftf_day_id,'-99999'),
	   NVL(trx.day_id , -99999),
	   NVL(trx.customer_id, -99999),
	   NVL(trx.position_id, -99999),
	   plan."NNI Channel",
	   ftf.status,
	    ftf.restriction_detail,
       ftf.zip,
	   trx.plan_id,
	   trx.brand_eligibility_flag,
       trx.first_time_writer_flag,
       trx.recurrent_writer_flag,
       trx.recurrent_writer_alt_flag,
	   trx.plan_exclusion_flag,
       ftf.snapshot_date ,
	   prod."Therapeutic Class",
	   md5('ftf'+cast (ftf_plan_id as varchar(15))+'d'+cast(ftf_day_id as varchar(15))) as ftf_performance_key,
	   trx.apportionment_trx * nvl(ftf.apportioned_weight ,1)  as ftf_trx
FROM rpt_apollo.ptab_fh_ftf_benefit_design_temp ftf
inner join rpt_apollo.ptab_f_sales_xptrx_terr_ftf_temp trx on trx.performance_key = ftf.performance_key
inner join rpt_apollo.v_tab_Geography_Attributes geo on geo.position_id = trx.position_id
inner join rpt_apollo.v_tab_Product_Attributes prod on prod.product_id = trx.product_id 
inner join rpt_apollo.v_tab_Managed_Care_Plan_Attributes plan on trx.plan_id = plan.plan_id
where trx.day_id >= ftf."16_week_ago_day_id" and trx.day_id <= ftf.ftf_day_id
)
;

VACUUM FULL rpt_apollo.ptab_fh_ftf_benefit_design_loading_temp to 99 PERCENT;

ANALYZE rpt_apollo.ptab_fh_ftf_benefit_design_loading_temp;		 
		
GRANT TRIGGER, RULE, SELECT, DELETE, UPDATE, REFERENCES, INSERT ON rpt_apollo.ptab_fh_ftf_benefit_design_loading_temp TO oasis_cdw_tst_procuser;
GRANT SELECT ON rpt_apollo.ptab_fh_ftf_benefit_design_loading_temp TO group oasis_cdw_tst_readuser_group;		

--Creating and/or loading final performance table for the Fingertip with all joins pre-calculated
CREATE TABLE IF NOT EXISTS rpt_apollo.ptab_fh_ftf_benefit_design_summary
(
   
   product_id               integer   ENCODE ZSTD NOT NULL,
   territory_type            varchar(100)  ENCODE ZSTD,
   ftf_plan_id              varchar(10)  NOT NULL,
   day_id                   integer    ENCODE ZSTD NOT NULL,
   customer_id              bigint   ENCODE ZSTD NOT NULL,
   position_id              bigint   ENCODE ZSTD NOT NULL,
   ftf_plan_name            varchar(100)    ENCODE ZSTD,
   nni_channel              varchar(50)  ENCODE ZSTD,
   class_nomenclature       varchar(100)    ENCODE ZSTD,
	subclass_nomenclature    varchar(100)    ENCODE ZSTD,
    material_restriction     char(1)    ENCODE ZSTD,
   restriction_detail       varchar(5000)    ENCODE ZSTD,
   status                   varchar(100)    ENCODE ZSTD,
   zip                      varchar(10)    ENCODE ZSTD,
    plan_id                   integer  NOT NULL     ,
    brand_eligibility_flag   varchar(100) ENCODE ZSTD,
   first_time_writer_flag      varchar(100) ENCODE ZSTD,
   recurrent_writer_flag     varchar(100)   ENCODE ZSTD,
   recurrent_writer_alt_flag    varchar(100)  ENCODE ZSTD,
   plan_exclusion_flag			varchar (100)  ENCODE ZSTD,
   snapshot_date            date     NOT NULL,
   ftf_form_strength      varchar(100)  ENCODE ZSTD,
   ftf_trx                  numeric(22,10)    ENCODE ZSTD
   


)
COMPOUND SORTKEY (snapshot_date,  territory_type, ftf_form_strength, nni_channel) 
;

INSERT INTO rpt_apollo.ptab_fh_ftf_benefit_design_summary
(
       product_id,
	   territory_type,
       ftf_plan_id,
	   day_id,
	   customer_id,
	   position_id,
	   ftf_plan_name,
	   nni_channel,
	   class_nomenclature,
	   subclass nomenclature,
	   material_restriction,
	    restriction_detail,
	     status,
       zip,
	   plan_id,
	   brand_eligibility_flag,
       first_time_writer_flag,
       recurrent_writer_flag,
       recurrent_writer_alt_flag,
	   plan_exclusion_flag,
       snapshot_date ,
	   ftf_form_strength,
	   ftf_trx
	   
	   

)

(sELECT
       ftf.product_id as product_id,
	   ftf.territory_type as "Territory Type",
       ftf_distinct.ftf_plan_id,
	   ftf.day_id,
	   ftf.customer_id,
	   ftf.position_id,
	   ftf_distinct.ftf_plan_name,
	   ftf.nni_channel ,
	   ftf_distinct.class_nomenclature, 
	   ftf_distinct.subclass_nomenclature ,
	   ftf_distinct.material_restriction,
	   ftf.restriction_detail ,
	   ftf.status,
       ftf.zip,
	   ftf.plan_id,
	   ftf.brand_eligibility_flag ,
       ftf.first_time_writer_flag ,
       ftf.recurrent_writer_flag ,
       ftf.recurrent_writer_alt_flag ,
	   ftf.plan_exclusion_flag ,
       ftf.snapshot_date  ,
	   ftf_distinct.ftf_form_strength,
	   sum(ftf_trx) as "FTF TRx"
FROM rpt_apollo.ptab_fh_ftf_benefit_design_loading_temp ftf
inner join rpt_apollo.ptab_fh_ftf_benefit_design_distinct_temp ftf_distinct on ftf_distinct.ftf_performance_key = ftf.ftf_performance_key
where ftf.therapeutic_class = ftf_distinct.therapeutic_class


group by
       ftf.product_id ,
	   ftf.territory_type ,
       ftf_distinct.ftf_plan_id,
	   ftf.day_id,
	   ftf.customer_id,
	   ftf.position_id,
	   ftf_distinct.ftf_plan_name,
	   ftf.nni_channel ,
	   ftf_distinct.class_nomenclature, 
	   ftf_distinct.subclass_nomenclature ,
	   ftf_distinct.material_restriction,
	   ftf.restriction_detail ,
	   ftf.status,
       ftf.zip,
	   ftf.plan_id,
	   ftf.brand_eligibility_flag ,
       ftf.first_time_writer_flag ,
       ftf.recurrent_writer_flag ,
       ftf.recurrent_writer_alt_flag ,
	   ftf.plan_exclusion_flag ,
       ftf.snapshot_date  ,
	   ftf_distinct.ftf_form_strength

)
;

VACUUM FULL rpt_apollo.ptab_fh_ftf_benefit_design_summary to 99 PERCENT;

ANALYZE rpt_apollo.ptab_fh_ftf_benefit_design_summary;		 
		
GRANT TRIGGER, RULE, SELECT, DELETE, UPDATE, REFERENCES, INSERT ON rpt_apollo.ptab_fh_ftf_benefit_design_summary TO oasis_cdw_tst_procuser;
GRANT SELECT ON rpt_apollo.ptab_fh_ftf_benefit_design_summary TO group oasis_cdw_tst_readuser_group;