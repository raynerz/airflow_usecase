import pandas as pd
import requests
import json
from io import StringIO

import subprocess
import sys
import os
import psycopg2

# Data Requirements

API_KEY = os.environ['TOM_TOM_API_KEY']

bern_lon = 7.434711
bern_lat = 46.945322

lausanne_lon = 6.631536
lausanne_lat = 46.520426

zh_lon = 8.540111
zh_lat = 47.377110

cities = [(bern_lon, bern_lat, "Bern", "BE"), (lausanne_lon, lausanne_lat, "Lausanne", "VD"), (zh_lon, zh_lat, "Zurich", "ZH")]


#### Main Python ETL Tasks

def collect_data(**op_kwargs):
    traffic_list = []

    for city in cities:
        response = \
            requests.get('https://api.tomtom.com/traffic/services/4/flowSegmentData/absolute/10/json?key='
                          + API_KEY + '&point=' + str(city[1]) + ','
                         + str(city[0]))
        traffic = json.loads(response.text)
        export = {
            'city': city[2],
            'canton': city[3],
            'latitude': city[1],
            'longitude': city[0],
            'currentSpeed': traffic['flowSegmentData']['currentSpeed'],
            'currentTravelTime': traffic['flowSegmentData'
                    ]['currentTravelTime'],
            'freeFlowSpeed': traffic['flowSegmentData']['freeFlowSpeed'
                    ],
            'freeFlowTravelTime': traffic['flowSegmentData'
                    ]['freeFlowSpeed'],
            }
        traffic_list.append(export)
    return traffic_list


def ingest_to_dwh(ti):

    ### Connecting to the database for ingestion
    conn = psycopg2.connect(
        host="postgres-dwh",
        database="airflow",
        user="airflow",
        password="airflow")

    cur = conn.cursor()

    ### Locating data from xcom_push

    data = ti.xcom_pull(task_ids="collect_data", key="return_value")

    ## getting the first record


    for record in data:
        # Persisting dim travel time
        cur.execute("INSERT INTO dim_travel_time VALUES(DEFAULT, %s, %s) RETURNING travel_key", (record['currentTravelTime'], record['freeFlowTravelTime']))
        travel_key = cur.fetchone()[0]

        # Persisting dim speed_key

        cur.execute("INSERT INTO dim_speed VALUES (DEFAULT, %s, %s) RETURNING speed_key", (record['currentSpeed'], record['freeFlowSpeed']))
        speed_key = cur.fetchone()[0]

        # Persisting dim fk_location

        cur.execute("INSERT INTO dim_location VALUES (DEFAULT, %s, %s, %s, %s) RETURNING loc_key", (record['city'], record['canton'], record['latitude'], record['longitude']))
        loc_key = cur.fetchone()[0]

        # Persisting fact tables

        cur.execute("INSERT INTO fact_traffic VALUES (DEFAULT, %s, %s, %s)", (speed_key, loc_key, travel_key))


    conn.commit()
    conn.close()
