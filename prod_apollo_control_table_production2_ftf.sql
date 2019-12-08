--Adding files to control load table for production 2 deployment

   INSERT INTO rpt_apollo.apollo_control_table
(
   sql_file_name                  ,
   load_sequence                  ,
   --Should be Y/N where Y will include file in the load
   include_in_load                ,
   schedule
)
VALUES
('ftf_load.sql',10,'Y','daily')

  

