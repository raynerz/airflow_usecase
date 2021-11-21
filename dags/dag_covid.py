# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import airflow
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from src.CovidHelper import collect_data, process_BE_cases, process_BE_tests

##########Building DAG

args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(7),
    'provide_context': True
}


dag = airflow.DAG(
    'dag_covid',
    schedule_interval="@daily",
    dagrun_timeout=timedelta(minutes=60),
    default_args=args,
    max_active_runs=1)

t1 = PythonOperator(task_id='collect_data',
                    python_callable=collect_data,
                    provide_context=True,
                    dag=dag)

t2_1 = PythonOperator(task_id='process_BE_cases',
                    python_callable=process_BE_cases,
                    provide_context=True,
                    dag=dag)

t2_2 = PythonOperator(task_id='process_BE_tests',
                    python_callable=process_BE_tests,
                    provide_context=True,
                    dag=dag)


t3 = PostgresOperator(
        task_id="ingest_to_dwh",
        postgres_conn_id='postgres_dwh',
        sql="sql/ingest_BE_COVID.sql",
        dag=dag
    )


t1 >> [t2_1, t2_2] >> t3


if __name__ == "__main__":
    dag.cli()
