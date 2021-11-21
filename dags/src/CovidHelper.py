import subprocess
import sys
import pandas as pd
import requests
from io import StringIO

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



##### Main Python Data Pipeline Tasks

#Create requests to the different sites in the OpenZH github repository in order to collect the covid information
def collect_data(**op_kwargs):
    cases_BE = requests.get(covid_cases_BE)
    tests_BE = requests.get(covid_tests_BE)

    data = {}

    data['BE'] = {"cases": cases_BE.text, "tests": tests_BE.text}



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
