--Creating de-normalized dimension table with all attributes for time
-- REM Add compression encoding
-- REM Add sort key & distribution 
-- Size is around 8 million rows
-- Add f_customer_attrib


--Adding Start Time
--Updating latest run stats to apollo control table
UPDATE rpt_apollo.apollo_control_table
set last_start_date = getdate() , status = 'Started'
WHERE sql_file_name = :'sqlfilename';



CREATE TABLE IF NOT EXISTS :loadschema.ptab_d_customer_full_new
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

--Loading Data Into Table In Sort Key Order
--REM Add Order By   
   INSERT INTO :loadschema.ptab_d_customer_full_new
(
   customer_id                    		  ,
   cus_source_unique_id                     ,
   cus_effective_start_date                 ,
   cus_end_date                             ,
   cus_status                               ,
   cus_first_name                           ,
   cus_middle_name                          ,
   cus_last_name                            ,
   cus_full_name                            ,
   cus_supertype                            ,
   cus_type                                 ,
   cus_subtype                              ,
   cus_npi_id                               ,
   cus_ims_id                               ,
   cus_hce_id                               ,
   cus_pr_spec_name                         ,
   cus_designation                          ,
   cus_deceased_date                        ,
   cus_job_title                            ,
   cus_gender                               ,
   cus_company_target_flag                  ,
   cus_best_addr_street                     ,
   cus_best_addr_city                       ,
   cus_best_addr_state                      ,
   cus_best_addr_zipcode                    ,
   cus_kol_flg                              ,
   cus_dea_number                           ,
   cus_me_number                            ,
   cus_ama_number                           ,
   cus_aoa_number                           ,
   cus_assmca_number                        ,
   cus_no_contact_indicator                 ,
   cus_restricted_rx_data_ind               ,
   cus_restricted_rx_data_effective_date    ,
   cus_current_source_unique_id             ,
   cus_designation_code                     ,
   cus_designation_name                     ,
   cus_degree_code                          ,
   cus_degree_name                          ,
   cus_pr_spec_code                         ,
   cus_pr_spec_group                        ,
   cus_professional_type         ,  
   diabetes_target_flag,
   aom_target_flag,
   cm_position_id,
   cm_masking_flag,
   cad_cust_addr_id,
   cad_address_id,
   cad_primary_addr,
   cad_secondary_addr,
   cad_city,
   cad_state,
   cad_country,
   cad_zip,
   cad_addr_type_code,
   cad_addr_type_name,
   cad_addr_status_code,
   cad_addr_status_name,
   cad_addr_effective_start_date,
   cad_addr_end_date,
   cad_best_addr_indicator,
   cad_preferred_mailing_addr_indicator,
   cad_addr_rank_number,
   cad_altrnt_addr_rank_number,
   cus_row_number,
   cm_row_number                ,
   batch_id,
   cad_row_number   
)
(


SELECT 
   cus.customer_id                    		  ,
   cus.source_unique_id                   ,
   cus.effective_start_date               ,
   cus.end_date                           ,
   cus.status                             ,
   cus.first_name                         ,
   cus.middle_name                        ,
   cus.last_name                          ,
   cus.full_name                          ,
   cus.supertype                          ,
   cus.type                               ,
   cus.subtype                            ,
   cus.npi_id                             ,
   cus.ims_id                             ,
   cus.hce_id                             ,
   cus.pr_spec_name                       ,
   cus.designation                        ,
   cus.deceased_date                      ,
   cus.job_title                          ,
   cus.gender                             ,
   cus.company_target_flag                ,
   cus.best_addr_street                   ,
   cus.best_addr_city                     ,
   cus.best_addr_state                    ,
   cus.best_addr_zipcode                  ,
   cus.kol_flg                            ,
   cus.dea_number                         ,
   cus.me_number                          ,
   cus.ama_number                         ,
   cus.aoa_number                         ,
   cus.assmca_number                      ,
   cus.no_contact_indicator               ,
   cus.restricted_rx_data_ind             ,
   cus.restricted_rx_data_effective_date  ,
   cus.current_source_unique_id           ,
   cus.designation_code                   ,
   cus.designation_name                   ,
   cus.degree_code                        ,
   cus.degree_name                        ,
   cus.pr_spec_code                       ,
   cus.pr_spec_group                      ,
   cus.professional_type             ,  
   diab_target.target_flag,
   aom_target.target_flag,   
   cm.position_id,
   cm.masking_flag,
   cad.cust_addr_id,
   cad.address_id,
   cad.primary_addr,
   cad.secondary_addr,
   cad.city,
   cad.state,
   cad.country,
   cad.zip,
   cad.addr_type_code,
   cad.addr_type_name,
   cad.addr_status_code,
   cad.addr_status_name,
   cad.addr_effective_start_date,
   cad.addr_end_date,
   cad.best_addr_indicator,
   cad.preferred_mailing_addr_indicator,
   cad.addr_rank_number,
   cad.altrnt_addr_rank_number,
   row_number () over (PARTITION BY cus.customer_id ORDER BY cm.position_id, cad.address_id) as cus_row_number,
   case when cm.customer_id is null then 0 else row_number () over (PARTITION BY cus.customer_id, cm.position_id ORDER BY cm.position_id, cad.address_id) end as cm_row_number,
   :batchid,
   case when cad.customer_id is null then 0 else row_number () over (PARTITION BY cus.customer_id, cad.address_id ORDER BY cm.position_id, cad.address_id) end as cad_row_number
FROM
:sourcedims.d_customer cus 
left join
:sourcedims.d_customer_masking cm
on cus.customer_id = cm.customer_id
left join :sourcedims.d_cust_addr cad
--Only pulling active customer addresses
on cus.customer_id = cad.customer_id and cad.addr_status_code = 'Active'
	 --Adding Target Flags  
  LEFT JOIN (select distinct customer_id,target_flag  from :sourcefacts.f_customer_alignment a,
:sourcedims.d_poa p where  a.poa_id=p.poa_id and P.active_flag='CURRENT'
and P.type='CURRENT SALES POA-DIAB' AND A.SALES_TEAM IN ('DCS', 'HSDCS', 'EDCS')
and alignment_level ='TERRITORY' and a.datasource_id in( 'PROF' ,'PROF_NBRX')) diab_target on diab_target.customer_id = cus.customer_id
LEFT JOIN (select distinct customer_id,target_flag  from :sourcefacts.f_customer_alignment a,
:sourcedims.d_poa p where  a.poa_id=p.poa_id and P.active_flag='CURRENT'
and P.type='CURRENT SALES POA-AOM' 
and alignment_level ='TERRITORY' and a.datasource_id in( 'PROF' ,'PROF_NBRX')) aom_target on aom_target.customer_id = cus.customer_id
)
;



VACUUM FULL :loadschema.ptab_d_customer_full_new TO 99 PERCENT;
ANALYZE :loadschema.ptab_d_customer_full_new;

GRANT TRIGGER, RULE, SELECT, DELETE, UPDATE, REFERENCES, INSERT ON :loadschema.ptab_d_customer_full_new TO :etluser;
GRANT SELECT ON :loadschema.ptab_d_customer_full_new TO :readonlyusers;
GRANT SELECT ON :loadschema.ptab_d_customer_full_new TO :readonlygroups;

DROP TABLE IF EXISTS :loadschema.ptab_d_customer_full_old;

ALTER TABLE :loadschema.ptab_d_customer_full RENAME TO ptab_d_customer_full_old;

ALTER TABLE :loadschema.ptab_d_customer_full_new RENAME TO ptab_d_customer_full;


--Updating latest run stats to apollo control table
UPDATE rpt_apollo.apollo_control_table
set status = 'Successfully Completed', batch_id = :batchid, last_loaded_date = getdate() ,total_records_loaded =  (SELECT count(*) from :loadschema.ptab_d_customer_full)
WHERE sql_file_name = :'sqlfilename';
