--Creating de-normalized dimension table with all attributes for time
-- REM Add compression encoding
-- REM Add sort key & distribution 
-- Sorted on the primary key
-- Flattened table size is less than 9000 rows recommend to distribute to all nodes   


--Adding Start Time
--Updating latest run stats to apollo control table
UPDATE rpt_apollo.apollo_control_table
set last_start_date = getdate() , status = 'Started'
WHERE sql_file_name = :'sqlfilename';


CREATE TABLE IF NOT EXISTS :loadschema.ptab_d_day_full_new
(
   sw_split_week_id     bigint ENCODE ZSTD,
   sw_week_num          varchar(6)  ENCODE ZSTD,
   sw_num_of_days       numeric(1)  ENCODE ZSTD,
   sw_start_date        date  ENCODE ZSTD,
   sw_end_date          date  ENCODE ZSTD,
   sw_week_end_date     date  ENCODE ZSTD,
   sw_calendar_month    date  ENCODE ZSTD,
   sw_month_end_date    date  ENCODE ZSTD,
   sw_year_month        varchar(6)  ENCODE ZSTD,
    day_id                  integer NOT NULL,
   d_day_dt                  date  ENCODE ZSTD,
   d_cal_mnth                numeric(2)  ENCODE ZSTD,
   d_cal_qtr                 numeric(1)  ENCODE ZSTD,
   d_cal_week                numeric(2)  ENCODE ZSTD,
   d_cal_year                numeric(4)  ENCODE ZSTD,
   d_day_name                varchar(30)  ENCODE ZSTD,
   d_mnth_name               varchar(30)  ENCODE ZSTD,
   d_mnth_strt_cal_dt_num    numeric(10)  ENCODE ZSTD,
   d_mnth_end_cal_dt_num     numeric(10)  ENCODE ZSTD,
   d_day_of_mnth             numeric(2)  ENCODE ZSTD,
   d_day_of_week             numeric(1)  ENCODE ZSTD,
   d_day_of_year             numeric(3)  ENCODE ZSTD,
   d_cal_dt_year_mnth        varchar(50)  ENCODE ZSTD,
   d_cal_dt_year_qtr         varchar(50)  ENCODE ZSTD,
   d_cal_dt_year_week        varchar(50)  ENCODE ZSTD,
   d_cal_dt_year             varchar(50)  ENCODE ZSTD,
   d_week_ending_date        date  ENCODE ZSTD,
   d_day_ago_num             integer  ENCODE ZSTD,
   d_day_ago_dt              date  ENCODE ZSTD,
   d_week_ago_num            integer  ENCODE ZSTD,
   d_week_ago_dt             date  ENCODE ZSTD,
   d_mnth_ago_num            integer  ENCODE ZSTD,
   d_mnth_ago_dt             date  ENCODE ZSTD,
   d_qtr_ago_num             integer  ENCODE ZSTD,
   d_qtr_ago_dt              date  ENCODE ZSTD,
   d_year_ago_num            integer  ENCODE ZSTD,
   d_year_ago_dt             date  ENCODE ZSTD,
   d_year_ago_strt_num       integer  ENCODE ZSTD,
   d_year_ago_end_num        integer  ENCODE ZSTD,
   d_mnth_ago_end_num        integer  ENCODE ZSTD,
   d_year_curr_strt_num      integer  ENCODE ZSTD,
   d_holiday_flag            integer  ENCODE ZSTD,
   d_poa                     varchar(30)  ENCODE ZSTD,
   d_poa_aom                 varchar(30)  ENCODE ZSTD,
   d_sales_poa               varchar(30)  ENCODE ZSTD,
   d_sales_poa_aom           varchar(30)  ENCODE ZSTD,
   batch_id                       int     ENCODE ZSTD,
   d_split_week_ending_date  date    ENCODE ZSTD       
   )
DISTSTYLE ALL
COMPOUND SORTKEY (day_id)
;
   
--Loading Data Into Table In Sort Key Order
--REM Add Order By   
   INSERT INTO :loadschema.ptab_d_day_full_new
   (
   sw_split_week_id     ,
   sw_week_num          ,
   sw_num_of_days       ,
   sw_start_date        ,
   sw_end_date          ,
   sw_week_end_date     ,
   sw_calendar_month    ,
   sw_month_end_date    ,
   sw_year_month        ,
    day_id                  ,
   d_day_dt                  ,
   d_cal_mnth                ,
   d_cal_qtr                 ,
   d_cal_week                ,
   d_cal_year                ,
   d_day_name                ,
   d_mnth_name               ,
   d_mnth_strt_cal_dt_num    ,
   d_mnth_end_cal_dt_num     ,
   d_day_of_mnth             ,
   d_day_of_week             ,
   d_day_of_year             ,
   d_cal_dt_year_mnth        ,
   d_cal_dt_year_qtr         ,
   d_cal_dt_year_week        ,
   d_cal_dt_year             ,
   d_week_ending_date        ,
   d_day_ago_num             ,
   d_day_ago_dt              ,
   d_week_ago_num            ,
   d_week_ago_dt             ,
   d_mnth_ago_num            ,
   d_mnth_ago_dt             ,
   d_qtr_ago_num             ,
   d_qtr_ago_dt              ,
   d_year_ago_num            ,
   d_year_ago_dt             ,
   d_year_ago_strt_num       ,
   d_year_ago_end_num        ,
   d_mnth_ago_end_num        ,
   d_year_curr_strt_num      ,
   d_holiday_flag            ,
   d_poa                     ,
   d_poa_aom                 ,
   d_sales_poa               ,
   d_sales_poa_aom           ,
   batch_id,
   d_split_week_ending_date
   )
   (
-- Matching the days to the split weeks with attributes.  Flattened table size is less than 9000 rows recommend to distribute to all nodes   
   SELECT
   sw.split_week_id     ,
   sw.week_num          ,
   sw.num_of_days       ,
   sw.start_date        ,
   sw.end_date          ,
   sw.week_end_date     ,
   sw.calendar_month    ,
   sw.month_end_date    ,
   sw.year_month        ,
    d.day_id                  ,
   d.day_dt                  ,
   d.cal_mnth                ,
   d.cal_qtr                 ,
   d.cal_week                ,
   d.cal_year                ,
   d.day_name                ,
   d.mnth_name               ,
   d.mnth_strt_cal_dt_num    ,
   d.mnth_end_cal_dt_num     ,
   d.day_of_mnth             ,
   d.day_of_week             ,
   d.day_of_year             ,
   d.cal_dt_year_mnth        ,
   d.cal_dt_year_qtr         ,
   d.cal_dt_year_week        ,
   d.cal_dt_year             ,
   d.week_ending_date        ,
   d.day_ago_num             ,
   d.day_ago_dt              ,
   d.week_ago_num            ,
   d.week_ago_dt             ,
   d.mnth_ago_num            ,
   d.mnth_ago_dt             ,
   d.qtr_ago_num             ,
   d.qtr_ago_dt              ,
   d.year_ago_num            ,
   d.year_ago_dt             ,
   d.year_ago_strt_num       ,
   d.year_ago_end_num        ,
   d.mnth_ago_end_num        ,
   d.year_curr_strt_num      ,
   d.holiday_flag            ,
   d.poa                     ,
   d.poa_aom                 ,
   d.sales_poa               ,
   d.sales_poa_aom           ,
   :batchid,
   d.split_week_ending_date
  FROM :sourcedims.d_day d left join :sourcedims.d_split_week sw
   ON d.week_ending_date=sw.end_date
   );
   
VACUUM FULL :loadschema.ptab_d_day_full_new TO 99 PERCENT;
ANALYZE :loadschema.ptab_d_day_full_new;

GRANT TRIGGER, RULE, SELECT, DELETE, UPDATE, REFERENCES, INSERT ON :loadschema.ptab_d_day_full_new TO :etluser;
GRANT SELECT ON :loadschema.ptab_d_day_full_new TO :readonlyusers;
GRANT SELECT ON :loadschema.ptab_d_day_full_new TO :readonlygroups;

DROP TABLE IF EXISTS :loadschema.ptab_d_day_full_old;

ALTER TABLE :loadschema.ptab_d_day_full RENAME TO ptab_d_day_full_old;

ALTER TABLE :loadschema.ptab_d_day_full_new RENAME TO ptab_d_day_full;

--Updating latest run stats to apollo control table
UPDATE rpt_apollo.apollo_control_table
set status = 'Successfully Completed', batch_id = :batchid, last_loaded_date = getdate() ,total_records_loaded =  (SELECT count(*) from :loadschema.ptab_d_day_full)
WHERE sql_file_name = :'sqlfilename';
