from datetime import datetime, timedelta
import pandas as pd
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import dag, task
from psycopg2.extras import execute_batch

default_args = {
    'owner': 'AntonM',
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
    }
schema = 'sales'

@dag(dag_id='dag_zone_economy_v02', 
     default_args=default_args, 
     start_date=datetime(2023, 4, 1),
    # end_date=datetime(2023, 4, 5),
     schedule_interval='@daily'
     )
def zone_economy_etl():

    @task()
    def zone_economy(execution_date=None, ti=None):
        yesterday_ds = execution_date - timedelta(days=1)
        ds = execution_date

        hook = PostgresHook(postgres_conn_id="postgres_localhost")
        conn = hook.get_conn()


        query = """
        WITH tgt_orders AS (SELECT order_id FROM "sales".orders)

        SELECT
            z.delivery_zone
            ,o.delivery_date as day
            ,sum(o.total_value) as Revenue
            ,sum(p.CoS) as Cost_of_sales
            ,sum(r.cost) as Delivery_cost
            ,count(distinct o.order_id) as Orders
            ,r2.cost * sum(p.quantity) as Store_cost
        FROM "sales".orders o
            JOIN "sales".zone z 
                ON o.zone_id = z.zone_id
            JOIN (SELECT order_id, sum(total_cost) as CoS, sum(quantity) as quantity 
                    FROM "sales".products
                    WHERE order_id IN (SELECT * FROM tgt_orders)
                    GROUP BY 1) as p
                ON o.order_id = p.order_id

            JOIN "sales".delivery_types d_t
                ON o.delivery_type = d_t.delivery_type

            JOIN "sales".store s 
                ON z.store_id = s.store_id
            JOIN "sales".resource r
                ON s.store_id = r.store_id AND d_t.name = r.resource_type
            JOIN "sales".resource r2
                ON s.store_id = r2.store_id 
                
        WHERE r2.resource_type = 'Store_picker' 
            AND o.delivery_date >= '{}' and o.delivery_date <= '{}'                   
        GROUP BY 1,2,r2.cost
        ORDER BY delivery_date
        """.format(yesterday_ds,ds)
        
        data = pd.read_sql_query(query, conn)
        data['gross_margin'] = data['revenue'] - data['cost_of_sales'] - data['delivery_cost'] - data['store_cost']
        return data

    @task
    def metrics(df, table = 'zone_economy', variable = 'day', schema = schema, drop = False):
        
        if df.empty == False:
            hook = PostgresHook(postgres_conn_id="postgres_localhost")
            conn = hook.get_conn()

            df[variable] = pd.to_datetime(df[variable])
            condition = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            df['version'] = condition
            df_columns = list(df)
            
            # create (col1,col2,...)
            columns = ",".join(df_columns)
            
            # create VALUES('%s', '%s",...) one '%s' per column
            values = "VALUES({})".format(",".join(["%s" for _ in df_columns]))
            
            # create INSERT INTO table (columns) VALUES('%s',...)
            cur = conn.cursor()
            
            # Delete existing rows based on date condition
            delete_stmt = "DELETE FROM {}.{} WHERE {} = ANY(%s)".format(schema, table, variable)
            cur.execute(delete_stmt, (df[variable].tolist(),))
            
            insert_stmt = "INSERT INTO {}.{} ({}) {}".format(schema, table, columns, values)
            
            if drop!=False: 
                drop_table = "DELETE from {}.{}".format(schema, table)
                cur.execute(drop_table)
            
            execute_batch(cur, insert_stmt, df.values)
            conn.commit()
            cur.close()
            conn.close()  

    metrics(df = zone_economy())
dag_zone_economy = zone_economy_etl()
