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
- Choices of flattening dimensional attributes
The most commonly selected dimensional attributes were placed directly in the fact table as well as being stored in the original dimensional table.  This was done to reduce the amount of joins required by the end user's queries and to increase performance.  Only the current dimensional attributes were treated in this manner so as to avoid the need the deduplicate records.  In cases where an attribute with multiple possible values for a single dimensional key was required to be stored in the fact table, a deduplication column was added and generated upon table load using an Over-Partition By statement sorted by Last Updated Date.  This deduplication column was then added to the sort key of the table to provide a means of quickly selecting the most current attribute.

- Management of Affiliations: Ragged Hiearchies & Hacking Tableau Referential Integrity
The affliations data presented a particularly cumbersome problem from a data modeling stand-point.  Although there were only 4 levels within the affiliations hierarchy, the lowest level member could be associated to one or more members of the 3rd, 2nd, or highest level.  In addition, the 3rd or 2nd level members could be optionally associated with a higher level thereby causing an inheritence relationship.  

This made roll-up aggregations within the Tableau tool particularly difficult since they would not be additive.  The normal way to address this problem would be to generate separate queries and reports for each level of the hierarchy the end user would want to see.  However, this would greatly negatively impact the ability to flow from top to bottom of hierachy within the visual experience.  Instead, a feature of Tableau's "Assume Referential Integrity" was exploited.  When Tableau assumes referential integrity the application will not make a join to secondary tables within a data source if no columns are selected from the secondary table.  Exploiting this logic, each level in the hierarchy would have its data stored in a separate table and joined to the base fact table.  Thus Tableau would dynamically duplicate and de-duplicate data depending upon what level of the hierarchy the user was viewing and the aggregate measure would always display the correct amounts.

- Distribution & Sort Keys
The make the data warehouse performant best practices for Redshift design were in general followed.  The largest dimensional table, customer in this case, was usually chosen as the distributing table and each fact table would match it on customer_id.  The remaining dimensional tables were distributed to all nodes.  Remember, the distribution key determines how data is distributed across nodes and strongly affects the performance of table joins.  For particular fact table, only one join can be optimized with distribution keys and the rest of the tables must be distributed to all nodes.  

Conversely the sort key determines how data is distributed within each node and strongly affect the performance of WHERE predicated and window functions.  The sort keys were chosen by trial and error based on user access patterns.  In general, the first sort key was chosen that minimizes the amount and number of cluster of data that must be scanned with each sort key chosen in order of increasing cardinality for each key.  If the data becomes so cardinal that essentially each value is nearly unique it serves little purpose to add it to the sort key.  The choice of sort keys is general an art and not exact and it's impossible to completely optimize for one query pattern without trading off performance for another query pattern.  It is possible to need two separate tables with exactly the same data but with different sort keys to meet the performance requirements of two separate groups (e.g. marketing and finance).  In my experience, creating two separate tables is preferable to using interleaved sort keys where essentially performance is sub-optimal for all.

- ELT Approach & General Lessons Learned
Because the data transformations involved aggregation and lookups rather than machine learning input-output operations or filtering operations, the ELT approach within the cloud makes sense where the database performs the transformations.  Performance was generally better when:
1. Generate a new table rather than performing updates or inserts on an existing table.
2. Alter Table Append is magically when the data doesn't need to remain in the source table after the operation.
3. Create multiple step staging tables rather than performing operations directly on a final table if possible.

Deployment Dependencies
1. To deploy this properly all the database (v_tab) views should be deployed first.

2. Then the dimensional tables (ptabs) deployed in no particular order of dependency.

3. The fact performance tables should be deployed after.  (Note Market Access been commenting out but please still feel free to review.)

4. (Assisting Scripts) - The three batch files included in the code directory will generated the needed load scripts above by appending the SQL files
into three files - one to deploy the v_tab views, one that will load all the dimensional ptabs, and one that will load the fact performance (ptabs)


Supplemental Files Included:
- Unit Testing Script for the dimensional tables / views which can be used the control tests for the dimensional tables
- Redshift helper query for debugging long running Tableau cursor queries
