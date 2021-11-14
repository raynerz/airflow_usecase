# Covid 19 DWH - Job Application

## requirements

- Docker v19+
- docker-compose v1.29+

## Starting the airflow service

If this is the first time starting the Airflow container

```
$ docker-compose up airflow-init
```

After initializating airflow

```
$ docker-compose up
```

Navigate to [locahost:8080](https:locahost:8080) and login using `username` "airflow" & `password` "airflow"

In order to remove the containers

```
$ docker-compose down -v --remove-orphans
````
