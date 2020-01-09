###Redshift Deployment for IQVIA Data Warehouse

Data Profiles:

- Facts: IQVIA Sales - Nbrx & XPtrx
Sales data comes from IQVIA, the pharmaceutical industry standard reporting organization.  XPTRx, exponent track plan prescriptions, shows total volume of prescriptions written by a prescriber.  NBRx, new to brand prescriptions, is a newer metric that takes into account if a prescription has been written for a patient within the last 52 weeks if it has not then they are considered "new to brand". 

- Facts: Internal Sales Activity
Activity fact data takes into account the myriad of sales actions taken that ultimately drive the sales listed above.  Activity can include phone calls, physician visits, presentations, and emails for example.

Common Dimensions:
  - Customers & Account and Affiliations Between
  - Managed Care Plan
  - Time: Day
  - Geography: Sales Territories

Data Design Choices:
Choices of flattening dimensional attributes 
Management of Affiliations: Ragged Hiearchies & Hacking Tableau Referential Integrity
Sort & Distribution keys and how Redshift works / ELT approach


Deployment Dependencies
1. To deploy this properly all the database (v_tab) views should be deployed first.

2. Then the dimensional tables (ptabs) deployed in no particular order of dependency.

3. The fact performance tables should be deployed after.  (Note Market Access been commenting out but please still feel free to review.)

4. (Assisting Scripts) - The three batch files included in the code directory will generated the needed load scripts above by appending the SQL files
into three files - one to deploy the v_tab views, one that will load all the dimensional ptabs, and one that will load the fact performance (ptabs)



Supplemental Files Included:
- Unit Testing Script for the dimensional tables / views which can be used the control tests for the dimensional tables
- Redshift helper query for debugging long running Tableau cursor queries
