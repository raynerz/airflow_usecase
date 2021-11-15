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
from io import StringIO

import subprocess
import sys

def install(package):
    subprocess.check_call([sys.executable, "-m", "pip", "install", package])

install("simplejson")

import simplejson as json

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

    data['BE'] = {"cases": cases_BE.text, "tests": tests_BE.text}
    #data['ZH'] = (cases_ZH.text, tests_ZH.text)
    #data['VD'] = (cases_VD.text, cases_VD.text)



    return data


def process_BE_cases(ti):
    data = ti.xcom_pull(task_ids="collect_data", key="return_value")

    bern_data = data["BE"]

    #Converting to StringIO so it can be read by pandas as dataframe
    str_cases = StringIO(bern_data["cases"])
    csv_cases = pd.read_csv(str_cases, header=0)

    #Sorting the dataframe

    csv_cases = csv_cases.sort_values(by=['date'], ascending=False)

    # In the case of the covid cases, the data only presents cumulative numbers. We are going to calculate infections and deaths by substracting the ones from the previous day

    cases_yesterday = csv_cases.iloc[1]
    cases_today = csv_cases.iloc[0]

    cases_today['infections'] = cases_today['ncumul_conf'] - cases_yesterday['ncumul_conf']
    cases_today['deaths'] = cases_today['ncumul_deceased'] - cases_yesterday['ncumul_deceased']

    cases_today['infections'] = cases_today['ncumul_conf'] - cases_yesterday['ncumul_conf']
    cases_today['deaths'] = cases_today['ncumul_deceased'] - cases_yesterday['ncumul_deceased']

    #locating cases
    cases_today = cases_today.to_dict()

    #Prepating data to ingest the data warehouse

    export = {
    "date" : cases_today['date'],
    "deaths": cases_today['deaths'],
    "infections": cases_today['infections'],
    "canton": "BE",
    }

    # Pushing data for cross process communications
    ti.xcom_push(key='date', value = export['date'])
    ti.xcom_push(key='deaths', value = export['deaths'])
    ti.xcom_push(key='infections', value = export['infections'])
    ti.xcom_push(key='canton', value= export['canton'])

    return export

def process_BE_tests(ti):
    data = ti.xcom_pull(task_ids="collect_data", key="return_value")

    bern_data = data["BE"]

    #Converting to StringIO so it can be read by pandas as dataframe
    str_tests = StringIO(bern_data["tests"])
    csv_tests = pd.read_csv(str_tests, header=0)

    #Sorting the dataframe

    csv_tests = csv_tests.sort_values(by=['start_date'], ascending=False)

    # Locating last day tests
    today_tests = csv_tests.iloc[0]
    today_tests = today_tests.to_dict()
    # Preparing data to ingest the data warehouse

    export = {
    "canton" : "BE",
    "date": today_tests['start_date'],
    "positive" : int(today_tests['positive_tests']),
    "negative" : today_tests['negative_tests'],
    "total" : int(today_tests['total_tests']),
    "positivity_rate" : float(today_tests['positivity_rate']),
    "source" : today_tests['source']
    }

    # Converting NaN values to null so they can be serialized by airflow libraries
    export = json.dumps(export, ignore_nan=True)
    export = json.loads(export)

    ti.xcom_push(key="canton", value = export['canton'])
    ti.xcom_push(key='date', value = export['date'])
    ti.xcom_push(key='positive', value = export['positive'])
    ti.xcom_push(key='negative', value = export['negative'])
    ti.xcom_push(key='total', value = export['total'])
    ti.xcom_push(key='positivity_rate', value = export['positivity_rate'])
    ti.xcom_push(key='source', value = export['source'])

    return export

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
