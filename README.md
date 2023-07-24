# Project Overview:
<p align="center">
  <img src="https://github.com/AntonMiniazev/Online_retail_Pipeline/blob/main/Others/data-pipeline-architecture-purpose.jpg" />
</p>

**Description:** This project involves the development of an automated dashboard for an online retail company, designed to monitor key sales metrics by delivery zones. We will simplify the pipeline preparation process for the online retail store using the following tools:
- Postgresql database (local for project simplification)
- Airflow (using Docker)
- Power BI

**Main Task:** The primary goal is to create Power BI dashboards that provide a simplified business overview focusing on sales metrics.

**Data Description:** A pre-generated set of tables in a PostgreSQL database. This data, which includes specific sales-related information such as orders, products, and sales, will be processed and organized into a designated table (**Proposed table**). This table will then serve as a consistent source for Power BI and will be regularly updated with new sales data using Airflow.

<p align="center">
  <img src="https://lucid.app/publicSegments/view/3564fc0c-9ef3-44a1-ba8b-819ac82206d3/image.png" />
</p>

# Step 1. Preparing Data

Orders and sale details (tables Orders and Products) are generated using script [Order_generator.ipynb](https://github.com/AntonMiniazev/Online_retail_Pipeline/blob/main/project_notebooks/Order_generator.ipynb).
Other tables include delivery information, associated with the order completion expenses.
All sourcing data tables are stored in csv files in folder [initial_data](https://github.com/AntonMiniazev/Online_retail_Pipeline/tree/main/project_notebooks)

# Step 2. Creating Database

Tables in postgresql database are created using [Database_initialization.ipynb](https://github.com/AntonMiniazev/Online_retail_Pipeline/blob/main/project_notebooks/Database_initialization.ipynb).

# Step 3. Creating DAG

Initial tables in database will be reprocessed into the Proposed table by Airflow. 
Process requirements:
- Scheduled daily to maintain up-to-date the Proposed table.
- Combine data from initial tables for orders from previous two days.
- Insert combined data into the Proposed table with specifics from **Data Description**.

Script for this process: [DAG](https://github.com/AntonMiniazev/Online_retail_Pipeline/blob/main/DAGs/dag_zone_economy.py)

# Step 4. Establishing Power BI report

Power BI is connected to postgresql database using ODBC driver. 
Proposed table is quered and reprocessed into a final report with followin pages:
- "Overview": Total revenue, AOV and Gross Margin by Delivery zones.
- "Revenue_by_zone": daily revenue dynamic with 14-day rolling average by zone.
- "Week -  Revenue_by_zone": weekly revenue dynamic with 2-week rolling average by delivery zone.
- "Week -  GM_by_zone": weekly gross margin dynamic with 2-week rolling average by delivery zone.
- "Week -  AOV_by_zone": weekly AOV dynamic by delivery zone.
- "Delivery Cost by zone": Delivery and Warehouse cost per order by zone dynamic.

Final Power BI report: [Sales and GM dynamic.pbix](https://github.com/AntonMiniazev/Online_retail_Pipeline/blob/main/Reports/Sales%20and%20GM%20dynamic.pbix)
