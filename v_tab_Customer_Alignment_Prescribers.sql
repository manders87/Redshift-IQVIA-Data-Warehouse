DROP VIEW IF EXISTS rpt_apollo.v_tab_Customer_Alignment_Prescribers;

CREATE VIEW rpt_apollo.v_tab_Customer_Alignment_Prescribers AS
(
select customer_id,
     position_id,
     territory_type 
from 
    rpt_apollo.ptab_f_customer_alignment f
    where alignment_name in ('CUST_TO_TERR', 'UNALIGNED_0_79_95', 'UNALIGNED_97_98_99', 'ZIP_TO_TERR')
group by 1,2,3
)
WITH NO SCHEMA BINDING;

GRANT TRIGGER, RULE, SELECT, DELETE, UPDATE, REFERENCES, INSERT ON rpt_apollo.v_tab_Customer_Alignment_Prescribers TO oasis_cdw_tst_procuser;
GRANT SELECT ON rpt_apollo.v_tab_Customer_Alignment_Prescribers TO group oasis_cdw_tst_readuser_group;	
GRANT SELECT ON rpt_apollo.v_tab_Customer_Alignment_Prescribers TO  oasis_cdw_tst_tbl_read_user;	

-- Check for Compile of VIEW
SELECT *
FROM rpt_apollo.v_tab_Customer_Alignment_Prescribers
LIMIT 1;
