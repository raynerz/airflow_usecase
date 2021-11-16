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

from __future__ import print_function
import airflow
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow import models
from airflow.settings import Session
import logging


args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(7),
    'provide_context': True
}


def initialize_dwh_conn():
    logging.info('Creating connections and definining sql path')

    session = Session()

    def create_new_conn(session):
        new_conn = models.Connection()
        new_conn.conn_id = "postgres_dwh"
        new_conn.conn_type = "postgres"
        new_conn.host =  "postgres-dwh"
        new_conn.port = "5432"
        new_conn.login = "airflow"
        new_conn.set_password("airflow")

        session.add(new_conn)
        session.commit()

    create_new_conn(session)
    session.commit()

    session.close()

dag = airflow.DAG(
    'init_dwh_conn',
    schedule_interval="@once",
    default_args=args,
    max_active_runs=1)

t1 = PythonOperator(task_id='initialize_dwh_conn',
                    python_callable=initialize_dwh_conn,
                    dag=dag)

t2 = PostgresOperator(task_id='build-tables',
                        postgres_conn_id = 'postgres_dwh',
                        autocommit = True,
                        sql = 'sql/dwh_tables.sql',
                        dag = dag)

t1 >> t2
