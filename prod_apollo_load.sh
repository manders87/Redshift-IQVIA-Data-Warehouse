################################################################## 
#  Usage: Used for loading Tableau rpt_apollo schema  
#  Author: Michael Anders (mikeanders87@gmail.com)  
#
#  Arguments: First and only argument is schema type eg. rpt, core, stage, etc. and must be provided when running the script
#  Usual schema type for rpt_apollo to source load from is rpt
#  If a schema other than rpt_apollo must be target loaded then the ?loadschema variables must be changed in the source apollo_env.sh
#
#  Notification Log - verbose status and executions are provided to the LOGFILE appended with execution date
#  Notification Email - emails can also be configured for both success status and error status - which also appends the detailed log
#                      separate groups are maintained for failure and success notification within the source file 
#            
#EXIT STATUS
#psql returns 
#0 to the shell if it finished normally, 
#1 if a fatal error of its own occurs (e.g. out of memory, file not found),  
#2 if the connection to the server went bad and the session was not interactive, and 
#3 if an error occurred in a script and the variable ON_ERROR_STOP was set
# 
#Script will load SQL files that must be named the same eg. "test.sql" in the rpt_apollo.apollo_control_table
#"Initial Scripts" will run once and then set their own rerun status to "N" in the control table
#Performance Table (ptab) scripts that should be executed regularly will always have a status of "Y" for rerun
#################################################################
#! /bin/bash

# -------------------------------------------------------------------
# Set environment variables
# -------------------------------------------------------------------
source /opt/infa_shared/cdw/Infa_Adm/Scripts/apollo/Master_script/apollo_env.sh
dt=$(date +"%Y%m%d%H%M%S")

LogFileName=$LOG_PATH/$loadschema_load_$dt.log

echo "Script starts at `date` "   >> $LogFileName

# -------------------------------------------------------------------
# Check that source schema has been passed as Argument
# -------------------------------------------------------------------

if [ "$#" -lt 1 ]; then

    echo 'Illegal number of parameters.  Kindly Pass source schema e.g. rpt or core to as Arguments'  >> $LogFileName
    exit 1
fi

sourceschematype=$1
echo 'Load for: ' $environment  >> $LogFileName
echo 'Schema type to be copied: ' $sourceschematype >> $LogFileName

# -------------------------------------------------------------------
# Get The List Of Tables to be loaded from apollo control table
# ------------------------------------------------------------------- 
tablesqlresult=$(psql  -X -b -A -h $host_name -U $user_name -d $db_name -v "ON_ERROR_STOP=1" -t -w -p $port -L $LogFileName -c "SELECT sql_file_name FROM rpt_apollo.apollo_control_table where include_in_load = 'Y' ORDER BY load_sequence ASC" 2>&1
)>> $LogFileName

IFS=', ' read -r -a tablesql<<< $tablesqlresult


number_of_tables=${#tablesql[@]} 

echo "Number of Tables to be load - $number_of_tables"

echo ${tablesql[@]}

echo 'The following tables will be loaded: '  >> $LogFileName
	for ((i=0;i<$number_of_tables;i++)); do
	tab_name=${tablesql[${i}]}
		echo  -e "$tab_name \n" >> $LogFileName
	done

# -------------------------------------------------------------------
# Getting ETL batch id
# ------------------------------------------------------------------- 	
batchid=$(psql  -X -b -A -h $host_name -U $user_name -d $db_name -v "ON_ERROR_STOP=1" -t -w -p $port -L $LogFileName -c "SELECT value FROM ${sourceschematype}_dims.d_lov where type = 'BATCH_ID'" 2>&1
) >> $LogFileName	

echo "Batch ID for current ETL load is $batchid" >> $LogFileName	

# -------------------------------------------------------------------
# Looping to load each table to rpt_apollo
# ------------------------------------------------------------------- 
for (( i=0;i<$number_of_tables;i++)); do 
    tab_name=${tablesql[${i}]}
	ftf_sql='ftf_load.sql'
##Special Handling for the Fingertip Formularies	
		echo "--------------------------------------------------------" >> $LogFileName    
		echo $tab_name
		if [ "$tab_name" == "$ftf_sql" ]; 
				then
				 ftf_max=$(psql  -X -b -A -h $host_name -U $user_name -d $db_name -v "ON_ERROR_STOP=1" -t -w -p $port -L $LogFileName -c "SELECT max(snapshot_date) FROM ${sourceschematype}_facts.fh_ftf_benefit_design" 2>&1
						)  >> $LogFileName
				 ftf_current=$(psql  -X -b -A -h $host_name -U $user_name -d $db_name -v "ON_ERROR_STOP=1" -t -w -p $port -L $LogFileName -c "SELECT max(snapshot_date) FROM $loadschema.ptab_fh_ftf_benefit_design_summary" 2>&1
						)  >> $LogFileName
					if [ "$ftf_max" != "$ftf_current" ];
					then 
					ftf_name=${ftf_current//"-"}
					echo "Renaming Existing Fingertip Summary to ptab_fh_ftf_benefit_design_summary_${ftf_name} to Make Way for New in $loadschema" >> $LogFileName    
					qstatus=$(psql  -X -b -A -h $host_name -U $user_name -d $db_name -v "ON_ERROR_STOP=1" -t -w -p $port -L $LogFileName -c "ALTER TABLE $loadschema.ptab_fh_ftf_benefit_design_summary RENAME TO ptab_fh_ftf_benefit_design_summary_${ftf_name}" 2>&1
						)  >> $LogFileName
					fi
				echo "Table "$tab_name" load to $loadschema schema started at `date`" >> $LogFileName      
			        qstatus=$(psql  -X -b -A -h $host_name -U $user_name -d $db_name -v "ON_ERROR_STOP=1"  -t -w -p $port -L $LogFileName -f $SQL_PATH'/'$tab_name -v sourcedims=${sourceschematype}_dims -v sourcefacts=${sourceschematype}_facts -v loadschema=$loadschema -v batchid=$batchid -v sqlfilename=$tab_name -v etluser=$user_name -v readonlyusers="${readonlyusers}" -v readonlygroups="${readonlygroups}" 2>&1
	                        )  >> $LogFileName
		else
		echo "Table "$tab_name" load to $loadschema schema started at `date`" >> $LogFileName      
		qstatus=$(psql  -X -b -A -h $host_name -U $user_name -d $db_name -v "ON_ERROR_STOP=1"  -t -w -p $port -L $LogFileName -f $SQL_PATH'/'$tab_name -v sourcedims=${sourceschematype}_dims -v sourcefacts=${sourceschematype}_facts -v loadschema=$loadschema -v batchid=$batchid -v sqlfilename=$tab_name -v etluser=$user_name -v readonlyusers="${readonlyusers}" -v readonlygroups="${readonlygroups}" 2>&1
	 )  >> $LogFileName
		fi
		COPY_ER=$?
			echo $qstatus >> $LogFileName
			echo "Table "$tab_name" load to $loadschema schema ended at `date`" >> $LogFileName    
			echo "--------------------------------------------------------" >> $LogFileName   
		if [[ $COPY_ER -ne 0 ]] 
			 then 	
			 break 
		fi  

		# -------------------------------------------------------------------
		# Block used for inserting record in history table
		# -------------------------------------------------------------------
		psql -X -A -h $host_name -U $user_name -d $db_name -t -w -p $port -L $LogFileName -c "insert into rpt_apollo.h_apollo_control_table (select * from rpt_apollo.apollo_control_table where sql_file_name = '$tab_name');" ;
		echo "--------------------------------------------------------" >> $LogFileName    
		echo "History records inserted successfully for file : "$tab_name"." >> $LogFileName  
done


#Check the load status and terminate the process if any error
if [[ $COPY_ER -ne 0 ]]
then
        echo "$loadschema load failure for $environment. Check the Informatica log or script log $LogFileName.  Exit Code : $COPY_ER "  >> $LogFileName
	    echo "Invoking email failure notification ... "  >> $LogFileName
		echo "Script ends at `date` "    >> $LogFileName
	    echo "The script error occurred at $tab_name."| mailx -a $LogFileName -s "Load of $loadschema schema for $environment Failed"  "$failed_email_group"      
        exit 1
else
        echo "Exit Code : $COPY_ER "   >> $LogFileName
 	 echo "The following tables loaded from $sourceschematype:"$'\n' $tablesqlresult|sed 's/.sql/@/g'  |tr "@" "\n" |sed 's/.sql/ /g'|mailx -s "Tables successfully loaded to $loadschema schema for $environment"  "$success_email_group"
        echo "Load of $loadschema schema for $environment completed successfully at `date`" >> $LogFileName
	

fi
echo "Script ends at `date` "   >> $LogFileName
echo "$environment load process of $loadschema schema finished at `date`" >> $LogFileName