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
from airflow.operators.postgres_operator import PostgresOperator
import pandas as pd
import requests

#Adresses where the data is stored
covid_cases_BE = "https://raw.githubusercontent.com/openZH/covid_19/master/fallzahlen_kanton_total_csv/COVID19_Fallzahlen_Kanton_BE_total.csv"
covid_cases_ZH = "https://raw.githubusercontent.com/openZH/covid_19/master/fallzahlen_kanton_total_csv/COVID19_Fallzahlen_Kanton_ZH_total.csv"
covid_cases_VD = "https://raw.githubusercontent.com/openZH/covid_19/master/fallzahlen_kanton_total_csv/COVID19_Fallzahlen_Kanton_VD_total.csv"

covid_tests_BE = "https://raw.githubusercontent.com/openZH/covid_19/master/fallzahlen_tests/fallzahlen_kanton_BE_tests.csv"
covid_tests_ZH = "https://raw.githubusercontent.com/openZH/covid_19/master/fallzahlen_tests/fallzahlen_kanton_ZH_tests.csv"
covid_tests_VD = "https://raw.githubusercontent.com/openZH/covid_19/master/fallzahlen_tests/fallzahlen_kanton_VD_tests.csv"


args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(7),
    'provide_context': True
}

##### Main Python ETL Tasks

#Create requests to the different sites in the OpenZH github repository in order to collect the covid information
def collect_data(**op_kwargs):
    cases_BE = requests.get(covid_cases_BE)
    cases_ZH = requests.get(covid_cases_ZH)
    cases_VD = requests.get(covid_cases_VD)

    tests_BE = requests.get(covid_tests_BE)
    tests_ZH = requests.get(covid_tests_ZH)
    tests_VD = requests.get(covid_tests_VD)

    data = {}

    data['BE'] = (cases_BE.content, tests_BE.content)
    data['ZH'] = (cases_ZH.content, tests_ZH.content)
    data['VD'] = (cases_VD.content, cases_VD.content)

    return data


def process_BE(**kwargs):
    data = {{t1.xcom_pull(task_ids=['collect_data'])}}

    bern_data = data['BE']

    #Converting to data frames
    csv_cases = pd.read_csv(bern_data[0])
    csv_tests = pd.read_csv(bern_data[1])

    #Sorting the dataframe

    csv_cases = csv_cases.sort_values(by=['date'], ascending=False)
    csv_tests = csv_cases.sort_values(by=['start_date'], ascending=False)

    # In the case of the covid cases, the data only presents cumulative numbers. We are going to calculate infections and deaths by substracting the ones from the previous day

    cases_yesterday = csv_cases.iloc[1]
    cases_today = csv_cases.iloc[0]

    cases_today['infections'] = cases_today['ncumul_conf'] - cases_yesterday['ncumul_conf']
    cases_today['deaths'] = cases_today['ncumul_deceased'] - cases_yesterday['ncumul_deceased']


def process_ZH():
    pass

def process_VD():
    pass


##########Building DAG

dag = airflow.DAG(
    'covid_ingestion',
    schedule_interval="@daily",
    dagrun_timeout=timedelta(minutes=60),
    default_args=args,
    max_active_runs=1)

t1 = PythonOperator(task_id='collect_data',
                    python_callable=collect_data,
                    provide_context=True,
                    dag=dag)

t2_1 = PythonOperator(task_id='process_BE',
                    python_callable=process_BE,
                    provide_context=True,
                    dag=dag)


t4 = PostgresOperator(
        task_id="ingest_to_dwh",
        postgres_conn_id='postgres_dwh',
        sql="""
            CREATE TABLE IF NOT EXISTS pet (
            pet_id SERIAL PRIMARY KEY,
            name VARCHAR NOT NULL,
            pet_type VARCHAR NOT NULL,
            birth_date DATE NOT NULL,
            OWNER VARCHAR NOT NULL);
          """,
         dag=dag
    )


t1 >> t2_1

if __name__ == "__main__":
    dag.cli()
