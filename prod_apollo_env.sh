# ---------------------------------------------------------------------------------------------------------------
#
#                               Environment file for MDW 
#
# Points to remember - Update the environment and login details while migrating this file to other environment.
#                    - Prior executing the ETL loads, ensure DB pointer variables ("host_name" and "user_name") are pointed to right redshift cluster
#		 
# ---------------------------------------------------------------------------------------------------------------

########################################## AWS REDSHIFT ENVIRONMENT ########################################### 
export host_name=***
export user_name=***
export db_name=tstcdw
export port=5439
export loadschema='***'
export PGPASSWORD='***'
export readonlyusers='oasis_cdw_tst_tbl_read_user' 
export readonlygroups='group oasis_cdw_tst_readuser_group'
export environment='Test - TSTCDW'

########################################## Notification ###########################################
##Separate multiple emails by a simple space
export success_email_group='***@novonordisk.com'
export failed_email_group='***@novonordisk.com'

###################################### APPLICATION FILE DIRECTORY DEFINITION ########################################
export ROOT_DIR=/opt/infa_shared/cdw/Infa_Adm/Scripts/apollo/;
export SCR_PATH=$ROOT_DIR/Master_script;
export TEMP_PATH=$SCR_PATH/Temp;
export SQL_PATH=$ROOT_DIR/SQLFiles;
export LOG_PATH=$ROOT_DIR/Logs;
export ZIP_PATH=$ROOT_DIR/SrcFiles/mdw;
