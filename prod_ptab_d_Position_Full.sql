--Creating de-normalized dimension table with all attributes for time
-- REM Add compression encoding
-- REM Add sort key & distribution 
-- Full table is less than 1 million.  Recommend to distribute to all nodes


--Adding Start Time
--Updating latest run stats to apollo control table
UPDATE rpt_apollo.apollo_control_table
set last_start_date = getdate() , status = 'Started'
WHERE sql_file_name = :'sqlfilename';



CREATE TABLE IF NOT EXISTS :loadschema.ptab_d_position_full_new
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
--Loading Data Into Table In Sort Key Order
--REM Add Order By   
   INSERT INTO :loadschema.ptab_d_position_full_new
(
   position_id           	 ,
   pos_source_unique_id      ,
   pos_name                  ,
   pos_type                  ,
   pos_parent_name           ,
   pos_type_flag             ,
   pos_effective_start_date  ,
   pos_end_date              ,
   pos_sales_force           ,
   pos_salesforce_value      ,
   pos_position_lvl          ,
   phi_position_hierarchy_id  ,
   phi_source_unique_id       ,
   phi_hierarchy_lvl          ,
   phi_top_hl_position_id     ,
   phi_top_hl_position_name   ,
   phi_top_hl_position_type   ,
   phi_hl1_position_id        ,
   phi_hl1_position_name      ,
   phi_hl1_position_type      ,
   phi_hl2_position_id        ,
   phi_hl2_position_name      ,
   phi_hl2_position_type      ,
   phi_hl3_position_id        ,
   phi_hl3_position_name      ,
   phi_hl3_position_type      ,
   phi_hl4_position_id        ,
   phi_hl4_position_name      ,
   phi_hl4_position_type      ,
   phi_hl5_position_id        ,
   phi_hl5_position_name      ,
   phi_hl5_position_type      ,
   phi_hl6_position_id        ,
   phi_hl6_position_name      ,
   phi_hl6_position_type      ,
   phi_hl7_position_id        ,
   phi_hl7_position_name      ,
   phi_hl7_position_type      ,
   phi_hl8_position_id        ,
   phi_hl8_position_name      ,
   phi_hl8_position_type      ,
   phi_effective_start_date   ,
   phi_end_date               ,
   phi_poa_name               ,
   phi_sales_force            ,
   phi_pod_primary            ,
   poa_poa_id             	            ,
   poa_source_unique_id    ,
   poa_type                ,
   poa_name                ,
   poa_start_date_id       ,
   poa_end_date_id         ,
   poa_prev_start_date_id  ,
   poa_prev_end_date_id    ,
   poa_num_of_months       ,
   poa_field_force         ,
   poa_active_flag         ,
   poa_prev_poa_name       ,
   psg_position_segment_id  ,
   psg_source_unique_id     ,
   psg_archetype_flag       ,
   eps_empl_pstn_id          ,
   eps_source_unique_id      ,
   eps_effective_start_date  ,
   eps_end_date              ,
   eps_active_flg            ,
   emp_employee_id       ,
   emp_source_unique_id  ,
   emp_emp_num           ,
   emp_emp_initials      ,
   emp_first_name        ,
   emp_middle_name       ,
   emp_last_name         ,
   emp_full_name         ,
   emp_addr_street       ,
   emp_addr_city         ,
   emp_addr_state        ,
   emp_addr_zip          ,
   emp_email             ,
   emp_title             ,
   emp_hire_date         ,
   emp_termination_date  ,
   emp_work_phone        ,
   emp_work_phone_ext    ,
   emp_active_status     ,
   pos_row_number	,
    psg_row_number,
   phi_row_number, 
   poa_row_number,
   eps_row_number,
   batch_id,
   emp_row_number
)
(
-- Full table is less than 1 million.  Recommend to distribute to all nodes
SELECT 
   pos.position_id           	 ,
   pos.source_unique_id      ,
   pos.name                  ,
   pos.type                  ,
   pos.parent_name           ,
   pos.type_flag             ,
   pos.effective_start_date  ,
   pos.end_date              ,
   pos.sales_force           ,
   pos.salesforce_value      ,
   pos.position_lvl          ,
   phi.position_hierarchy_id  ,
   phi.source_unique_id       ,
   phi.hierarchy_lvl          ,
   phi.top_hl_position_id     ,
   phi.top_hl_position_name   ,
   phi.top_hl_position_type   ,
   phi.hl1_position_id        ,
   phi.hl1_position_name      ,
   phi.hl1_position_type      ,
   phi.hl2_position_id        ,
   phi.hl2_position_name      ,
   phi.hl2_position_type      ,
   phi.hl3_position_id        ,
   phi.hl3_position_name      ,
   phi.hl3_position_type      ,
   phi.hl4_position_id        ,
   phi.hl4_position_name      ,
   phi.hl4_position_type      ,
   phi.hl5_position_id        ,
   phi.hl5_position_name      ,
   phi.hl5_position_type      ,
   phi.hl6_position_id        ,
   phi.hl6_position_name      ,
   phi.hl6_position_type      ,
   phi.hl7_position_id        ,
   phi.hl7_position_name      ,
   phi.hl7_position_type      ,
   phi.hl8_position_id        ,
   phi.hl8_position_name      ,
   phi.hl8_position_type      ,
   phi.effective_start_date   ,
   phi.end_date               ,
   phi.poa_name               ,
   phi.sales_force            ,
   phi.pod_primary            ,
   poa.poa_id             	            ,
   poa.source_unique_id    ,
   poa.type                ,
   poa.name                ,
   poa.start_date_id       ,
   poa.end_date_id         ,
   poa.prev_start_date_id  ,
   poa.prev_end_date_id    ,
   poa.num_of_months       ,
   poa.field_force         ,
   poa.active_flag         ,
   poa.prev_poa_name       ,
   psg.position_segment_id  ,
   psg.source_unique_id     ,
   psg.archetype_flag       ,
   eps.empl_pstn_id          ,
   eps.source_unique_id      ,
   eps.effective_start_date  ,
   eps.end_date              ,
   eps.active_flg            ,
   emp.employee_id       ,
   emp.source_unique_id  ,
   emp.emp_num           ,
   emp.emp_initials      ,
   emp.first_name        ,
   emp.middle_name       ,
   emp.last_name         ,
   emp.full_name         ,
   emp.addr_street       ,
   emp.addr_city         ,
   emp.addr_state        ,
   emp.addr_zip          ,
   emp.email             ,
   emp.title             ,
   emp.hire_date         ,
   emp.termination_date  ,
   emp.work_phone        ,
   emp.work_phone_ext    ,
   emp.active_status     ,
   row_number () over (partition by pos.position_id order by poa.active_flag asc, poa.poa_id desc nulls last, phi.position_hierarchy_id desc nulls last ,psg.position_segment_id,  eps.employee_id, emp.employee_id) as pos_row_number		  ,
   case when psg.position_id is null then 0 else row_number () over (partition by pos.position_id , psg.position_segment_id order by poa.active_flag asc, poa.poa_id desc nulls last, phi.position_hierarchy_id desc nulls last ,psg.position_segment_id,  eps.employee_id, emp.employee_id) end as psg_row_number		  ,
   case when phi.position_id is null then 0 else row_number () over (partition by pos.position_id , phi.position_hierarchy_id order by poa.active_flag asc, poa.poa_id desc nulls last, phi.position_hierarchy_id desc nulls last ,psg.position_segment_id,  eps.employee_id, emp.employee_id) end as phi_row_number	,
   case when poa.poa_id is null then 0 else row_number () over (partition by pos.position_id, poa.poa_id   order by poa.active_flag asc, poa.poa_id desc nulls last, phi.position_hierarchy_id desc nulls last ,psg.position_segment_id,  eps.employee_id, emp.employee_id ) end as poa_row_number	,
   case when eps.position_id is null then 0 else row_number () over (partition by pos.position_id , eps. empl_pstn_id order by poa.active_flag asc, poa.poa_id desc nulls last, phi.position_hierarchy_id desc nulls last ,psg.position_segment_id,  eps.employee_id, emp.employee_id) end as eps_row_number	,
   :batchid,
   case when emp.employee_id is null then 0 else row_number () over (partition by pos.position_id, emp.employee_id order by poa.active_flag asc, poa.poa_id desc nulls last, phi.position_hierarchy_id desc nulls last ,psg.position_segment_id,  eps.employee_id, emp.employee_id) end as emp_row_number
    
FROM
:sourcedims.d_position pos
--Keep for Obesity Sales Flags
LEFT JOIN :sourcedims.d_position_segment psg
ON pos.position_id = psg.position_id
LEFT JOIN
:sourcedims.d_position_hierarchy phi 
ON phi.position_id = pos.position_id
left JOIN :sourcedims.d_poa poa 
on poa.poa_id = phi.poa_id
LEFT JOIN
--Keep the employee tables
:sourcedims.d_empl_pstn eps
ON pos.position_id = eps.position_id 
left join :sourcedims.d_employee emp 
on eps.employee_id = emp.employee_id
);

   
GRANT TRIGGER, RULE, SELECT, DELETE, UPDATE, REFERENCES, INSERT ON :loadschema.ptab_d_position_full_new TO :etluser;
GRANT SELECT ON :loadschema.ptab_d_position_full_new TO :readonlyusers;
GRANT SELECT ON :loadschema.ptab_d_position_full_new TO :readonlygroups;


VACUUM FULL :loadschema.ptab_d_position_full_new TO 99 PERCENT;
ANALYZE :loadschema.ptab_d_position_full_new;


DROP TABLE IF EXISTS :loadschema.ptab_d_position_full_old;

ALTER TABLE :loadschema.ptab_d_position_full RENAME TO ptab_d_position_full_old;

ALTER TABLE :loadschema.ptab_d_position_full_new RENAME TO ptab_d_position_full;


--Updating latest run stats to apollo control table
UPDATE rpt_apollo.apollo_control_table
set status = 'Successfully Completed', batch_id = :batchid, last_loaded_date = getdate() ,total_records_loaded =  (SELECT count(*) from :loadschema.ptab_d_position_full)
WHERE sql_file_name = :'sqlfilename';

