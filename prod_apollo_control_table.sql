
--Creating de-normalized dimension table with all attributes for time
-- REM Add compression encoding
-- REM Add sort key & distribution
-- Flattened Account Table is around 5 million rows vs. 1 million for d_account and distributed to all nodes 


CREATE TABLE IF NOT EXISTS rpt_apollo.apollo_control_table
(
--Table identity field that is automatically assigned by database
   apollo_control_table_id        int   IDENTITY ENCODE ZSTD  NOT NULL ,
--SQL file that is to be loaded that should be the exact name of the file without path and suffixed with sql eg. "test.sql"   
   sql_file_name                  varchar(max)  ENCODE ZSTD NOT NULL,
--Used to manage the sequence in which scripts will load.  Can be a decimal or negative value.  Eg. 1 will be executed before 2 or 2.5   
   load_sequence                  decimal(5,2) ENCODE ZSTD NOT NULL,
--Should be Y/N where Y will include file in the load.  Some scripts only meant to be run once will set their own "include_in_load" to 'N' after they run
   include_in_load                varchar(1)  ENCODE ZSTD NOT NULL,  
--Optional fields that be updated for additional information.  Most of the ptab scripts will updated these values themselves   
   last_start_date                timestamptz      ENCODE ZSTD,
   last_loaded_date               timestamptz      ENCODE ZSTD,
   schedule                       varchar(50)      ENCODE ZSTD,
   total_records_loaded           bigint     ENCODE ZSTD    ,
   batch_id                       int        ENCODE ZSTD
)
;
--Loading Data Into Table In Sort Key Order
--REM Add Order By   
   INSERT INTO rpt_apollo.apollo_control_table
(
   sql_file_name                  ,
   load_sequence                  ,
   --Should be Y/N where Y will include file in the load
   include_in_load                ,
   schedule
)
VALUES
('Production_Deploy_1.sql',0,'Y',''),
('ptab_d_Account_Full.sql',1,'Y','daily'),
('ptab_d_Customer_Full.sql',2,'Y','daily'),
('ptab_d_Day_Full.sql',3,'Y','daily'),
('ptab_d_Plan_Full.sql',4,'Y','daily'),
('ptab_d_Position_Full.sql',5,'Y','daily'),
('ptab_d_Product_Full.sql',6,'Y','daily'),
('ptab_F_Interaction_Call.sql',7,'Y','daily'),
('ptab_Sales_Nbrx_Terr.sql',8,'Y','daily'),
('ptab_Sales_Xptrx_Terr.sql',9,'Y','daily');
  


GRANT TRIGGER, RULE, SELECT, DELETE, UPDATE, REFERENCES, INSERT ON rpt_apollo.apollo_control_table TO oasis_cdw_tst_procuser;
GRANT SELECT ON rpt_apollo.apollo_control_table TO oasis_cdw_tst_tbl_read_user;
GRANT SELECT ON rpt_apollo.apollo_control_table TO group oasis_cdw_tst_readuser_group;

--Creating History Control Table

CREATE TABLE IF NOT EXISTS rpt_apollo.h_apollo_control_table
(
--Table identity field that is automatically assigned by database
   h_apollo_control_table_id        int   ENCODE ZSTD  NOT NULL ,
--SQL file that is to be loaded that should be the exact name of the file without path and suffixed with sql eg. "test.sql"   
   sql_file_name                  varchar(max)  ENCODE ZSTD NOT NULL,
--Used to manage the sequence in which scripts will load.  Can be a decimal or negative value.  Eg. 1 will be executed before 2 or 2.5   
   load_sequence                  decimal(5,2) ENCODE ZSTD NOT NULL,
--Should be Y/N where Y will include file in the load.  Some scripts only meant to be run once will set their own "include_in_load" to 'N' after they run
   include_in_load                varchar(1)  ENCODE ZSTD NOT NULL,  
--Optional fields that be updated for additional information.  Most of the ptab scripts will updated these values themselves   
   last_start_date                timestamptz      ENCODE ZSTD,
   last_loaded_date               timestamptz       ENCODE ZSTD,
   schedule                       varchar(50)      ENCODE ZSTD,
   total_records_loaded           bigint     ENCODE ZSTD   ,
   batch_id                       int        ENCODE ZSTD
)
COMPOUND SORTKEY (last_loaded_date) 
;


GRANT TRIGGER, RULE, SELECT, DELETE, UPDATE, REFERENCES, INSERT ON rpt_apollo.h_apollo_control_table TO oasis_cdw_tst_procuser;
GRANT SELECT ON rpt_apollo.h_apollo_control_table TO oasis_cdw_tst_tbl_read_user;
GRANT SELECT ON rpt_apollo.h_apollo_control_table TO group oasis_cdw_tst_readuser_group;


