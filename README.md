# Project Overview:

**Description:** This project involves the development of an automated dashboard for an online retail company, designed to monitor key sales metrics by delivery zones. We will simplify the pipeline preparation process for the online retail store using the following tools:
- Postgresql database
- Airflow
- Power BI

**Main Task:** The primary goal is to create Power BI dashboards that provide a simplified business overview focusing on sales metrics.

**Data Description:** A pre-generated set of tables in a PostgreSQL database. This data, which includes specific sales-related information such as orders, products, and sales, will be processed and organized into a designated table. This table will then serve as a consistent source for Power BI and will be regularly updated with new sales data using Airflow.

<p align="center">
  <img src="https://lucid.app/publicSegments/view/3564fc0c-9ef3-44a1-ba8b-819ac82206d3/image.png" />
</p>

# Preparing Data

Orders and sale details (tables Orders and Products) are generated using script [Order_generator.ipynb](https://github.com/AntonMiniazev/Online_retail_Pipeline/blob/main/project_notebooks/Order_generator.ipynb).
Other tables include delivery information, associated with the order completion expenses.
All sourcing data tables are stored in csv files in folder [initial_data](https://github.com/AntonMiniazev/Online_retail_Pipeline/tree/main/project_notebooks)

# Creating Database

Databaze init


