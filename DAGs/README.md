# Content

DAD dag_zone_economy.py connects to a database using established in Airflow connection and creates two tasks "zone_economy" and "metrics":
- Task 1: "zone_economy" is a query from a database that consolidate order data for two previous dates for the Proposed table.
- Task 2: "metrics" takes table output from Task 1 and updates the Proposed table

DAG is scheduled as daily.
