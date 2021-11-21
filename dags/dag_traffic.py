import airflow
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from src.TrafficHelper import collect_data, ingest_to_dwh

#### Building dag
args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(6),
    'provide_context': True
}

dag = airflow.DAG(
    'dag_traffic',
    schedule_interval="*/5 * * * *", #Every 5 minutes
    dagrun_timeout=timedelta(minutes=60),
    default_args=args,
    max_active_runs=1)

t1 = PythonOperator(task_id='collect_data',
                    python_callable=collect_data,
                    provide_context=True,
                    dag=dag)

t2 = PythonOperator(task_id='ingest_to_dwh',
                    python_callable=ingest_to_dwh,
                    provide_context=True,
                    dag=dag)
t1 >> t2
