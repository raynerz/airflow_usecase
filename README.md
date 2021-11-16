# Covid 19 DWH - Job Application

This project is part of a job application, it's a dummy data warehouse connected do different automation and data analysis services. The project contain the following modules:

- Airflow - ETL and Task automation
- Metabase - Data Analysis
- Postgres - Hosts the DWH


## Requirements

- Docker v19+
- docker-compose v1.29+

## How to run this project

1. If this is the first time starting Airflow

```
$ ./run_first.sh
```

2. After initializating airflow

```
$ ./run_second.sh
```
## How to use this project

1. Navigate to [locahost:8080](http:locahost:8080) and login using username `airflow` & password `airflow`
2. Run the DAG called 'init_dwh_conn' in order to instantiate the DWH dwh_tables
3. Run the rest of the dags to ingest the DWH
4. Navigate to the metabase service at [localhost:3000](http:localhost:3000)
5. Create an account to access the metabase interface
6. Connect to a postgres database at: name: `airflow`, host: `postgres-dwh`, port: `5432`, username: `airflow`, password: `airflow`, database name: `airflow`

## Remove the containers and volumes


B
B
A
A
A


A
```
$ ./clean.sh
````
