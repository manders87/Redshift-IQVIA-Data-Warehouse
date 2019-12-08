##################################################################
#  Usage: Used for loading Tableau rpt_apollo schema 
#  Author: Michael Anders (mikeanders87@gmail.com)  
#
#  For verifying the compile of all Apollo views
#################################################################
#! /bin/bash

# -------------------------------------------------------------------
# Set environment variables
# -------------------------------------------------------------------
source /opt/infa_shared/cdw/Infa_Adm/Scripts/apollo/Master_script/apollo_env.sh
dt=$(date +"%Y%m%d%H%M%S")

SQLFileName=$LOG_PATH/view_validation_$dt.log

user_name=oasis_cdw_prd_tblreaduser_apollo

host_name=novo-dw-redshift.clo2x7mxqgak.us-east-1.redshift.amazonaws.com
db_name=prdcdw
PGPASSWORD='ApoLLoPrdTbl@#832'






# -------------------------------------------------------------------
# Get The List Of Views to be Validated
# ------------------------------------------------------------------- 
tablesqlresult=$(psql  -X -b -A -h $host_name -U $user_name -d $db_name -t -v "ON_ERROR_STOP=1" -w -p $port -L $SQLFileName -c "select distinct view_name from pg_get_late_binding_view_cols() cols(view_schema name, view_name name, col_name name, col_type varchar, col_num int) where view_schema = 'rpt_apollo'" 2>&1
)>> $SQLFileName
 


IFS=', ' read -r -a tablesql<<< $tablesqlresult


number_of_views=${#tablesql[@]} 

echo "Number of Views to be tested - $number_of_views"

echo ${tablesql[@]}

echo 'The following views will be validated: '  >> $SQLFileName
	for ((i=0;i<$number_of_views;i++)); do
	tab_name=${tablesql[${i}]}
		echo  -e "$tab_name \n" >> $SQLFileName
	done


# -------------------------------------------------------------------
# Looping to test each view
# ------------------------------------------------------------------- 
for (( i=0;i<$number_of_views;i++)); do 
    tab_name=${tablesql[${i}]}
    echo "--------------------------------------------------------" >> $SQLFileName    
    qstatus=$(psql  -X -b -A -h $host_name -U $user_name -d $db_name -v "ON_ERROR_STOP=1" -t -w -p $port -L $SQLFileName -c "select * from rpt_apollo.$tab_name LIMIT 1")  >> $SQLFileName
COPY_ER=$?
echo "Exit Code:$COPY_ER" >> $SQLFileName
    echo $qstatus >> $SQLFileName
    echo "--------------------------------------------------------" >> $SQLFileName
done

# -------------------------------------------------------------------
# Performing Unit Test Script for Dimensions
# ------------------------------------------------------------------- 	
qstatus=$(psql  -X -b -A -h $host_name -U $user_name -d $db_name -v "ON_ERROR_STOP=1" -t -w -p $port -L $SQLFileName << EOF  

	select count(*),'v_Time'
FROM rpt_apollo.v_tab_Time_Period_Attributes
union
select count(*), 'd_day'
FROM rpt_dims.d_day;


select count(*), count (plan_id), 'v_Managed_Care_Plan'
from rpt_apollo.v_tab_Managed_Care_Plan_Attributes
union
select count(*), count(plan_id), 'd_plan'
from rpt_dims.d_plan;



select count(*),'vtab_product', count(distinct product_id)
FROM rpt_apollo.v_tab_Product_Attributes
union
select count (*), 'd_product+hier', count (distinct prod.product_id)
from rpt_dims.d_product prod inner join rpt_dims.d_product_hierarchy prh on prod.product_id = prh.product_id;


select count(*), count(distinct ims_id) as distinct_ims_id, count(distinct customer_id), 'd_customer'
from rpt_dims.d_customer
union
select count(*), count(distinct "ims id")  as distinct_ims_id, count(distinct customer_id), 'vtab_Prescriber'
from rpt_apollo.v_tab_Prescriber_Attributes;

select count(*), count(distinct source_unique_id), count(distinct account_id), 'd_account'
from rpt_dims.d_account
union
select count(*), count(distinct "Account CDM Id"), count(distinct account_id), 'vtab_Account'
from rpt_apollo.v_tab_Account_Attributes;

--Position Hierarchy Check on Performance Table.  Test should return nothing
--select count (*), 'per_pos'
select position_id, pos_type, phi_position_hierarchy_id, poa_poa_id
FROM rpt_apollo.ptab_d_position_full
where poa_active_flag = 'CURRENT' and phi_row_number < 2 
minus
select pos.position_id, pos.type, phi.position_hierarchy_id, poa.poa_id
from rpt_dims.d_position pos inner join rpt_dims.d_position_hierarchy phi on pos.position_id = phi.position_id
inner join rpt_dims.d_poa poa on phi.poa_id = poa.poa_id and poa.active_flag = 'CURRENT'
union
select pos.position_id, pos.type, phi.position_hierarchy_id, poa.poa_id
from rpt_dims.d_position pos inner join rpt_dims.d_position_hierarchy phi on pos.position_id = phi.position_id
inner join rpt_dims.d_poa poa on phi.poa_id = poa.poa_id and poa.active_flag = 'CURRENT'
minus
select position_id, pos_type, phi_position_hierarchy_id, poa_poa_id
FROM rpt_apollo.ptab_d_position_full
where poa_active_flag = 'CURRENT' and phi_row_number < 2 ;

select count(*), count(distinct position_id), count("territory id"), 'vtab_Geography'
from rpt_apollo.v_tab_Geography_Attributes
union
select count(*), count(distinct pos.position_id), count(phi.hl4_position_id), 'd_position+hier+currentpoa'
from rpt_dims.d_position pos
inner join rpt_dims.d_position_hierarchy phi on pos.position_id = phi.position_id
inner join rpt_dims.d_poa poa on poa.poa_id = phi.poa_id and poa.active_flag = 'CURRENT';

select count(*), count(distinct position_id), 'vtab_Geography_AllPoas_LatestPos'
from rpt_apollo.v_tab_Geography_Attributes_All_POAs
union
select count(*), count(distinct position_id), 'd_position'
from rpt_dims.d_position;
EOF
)>>$SQLFileName

echo $COPY_ER >> $SQLFileName
    echo $qstatus >> $SQLFileName