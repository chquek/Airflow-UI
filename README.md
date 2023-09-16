# Airflow-UI

Kinisi Airflow UI


# Requirements 

A VM with the following installed :

- docker
- [docker compose](https://docs.docker.com/compose/install/)


## Folders /home/kinisi

A user /home/kinisi with the following directories :

Folder | Description | Exported to 
--- | --- | --- |
dags | Location of generated DAGs | designer:/home/kinisi/dags , notebook:/home/kinisi/dags , and airflow:/home/airflow/dags
certs | Database SSL certificates | notebook:/home/kinisi/certs` , airflow:/certs 
envfile | Version of airflow ,   id/pw for UI .  Source this file before starting docker |
logs | Log files for airflow task execution |  designer:/home/kinisi/logs , notebook:/home/kinisi/logs , airflow:/home/airflow/klogs
system | System modules | designer:/home/kinisi/system , airflow:/home/kinisi/system
user | User defined modules |  airflow:/home/airflow/udm , notebook:/home/kinisi/udm
workarea | Folders for working files | airflow:/workarea , notebood:/home/kinisi/workarea
DONOTREMOVE/cert | http ssl certificates and nginx configuration | nginx:/cert
DONOTREMOVE/nginx.conf | nginx configuration | nginx:/etc/nginx/conf.d/nginx.conf

## Sample envfile 

Source this file.  Docker-compose will use these variables

```
export AIRFLOW_VERSION=2.5.3
export USERNAME=<whatever>
export USERPASS=<whatever>
export SERVERNAME=<hostname>
# If validate is set to NO , no prompt for ID/PW
export VALIDATE=YES       
```


## docker compose 

``` yaml
version: '3.1'

services:

  nginx :

    image : nginx
    depends_on : [ designer , airflow , notebook ]
    volumes:
      - ./DONOTREMOVE/nginx.conf:/etc/nginx/conf.d/nginx.conf
      - ./DONOTREMOVE/cert:/cert
    ports :
      - 443:443
    command : [ "nginx" , "-g" , "daemon off;" ]

  designer :
    image : kinisi/designer
    environment :
      - BASIC_USERNAME=${USERNAME}
      - BASIC_PASSWORD=${USERPASS}
      - SERVER_NAME=${SERVERNAME}
      - VALIDATE=${VALIDATE}
      - GENALL=${GENALL}
      - DB=/home/kinisi/system/kinisi.db
    volumes:
      - ./system:/home/kinisi/system
      - ./dags:/home/kinisi/dags
      - ./logs:/home/kinisi/logs

  postgres:
    image: postgres:9.6
    environment:
      - POSTGRES_USER=airflow
      - POSTGRES_PASSWORD=airflow
      - POSTGRES_DB=airflow
      - PGDATA=/var/lib/postgresql/data/pgdata

  airflow :
    image: kinisi/airflow:${AIRFLOW_VERSION}
    depends_on:
      - postgres
    environment:
      - LOAD_EX=n
      - FERNET_KEY=46BKJoQYlPPOexq0OhDZnIlNepKFf87WFwLbfzqDDho=
      - EXECUTOR=Local
      - AIRFLOW__WEBSERVER__BASE_URL=http://x/airflow
    command: webserver
    volumes:
      - ./dags:/home/airflow/dags
      - ./user:/home/airflow/udm
      - ./workarea:/workarea
      - ./system:/home/kinisi/system
      - ./logs:/home/airflow/klogs
      - ./certs:/certs

  notebook :
    image: jupyter/base-notebook
    build :
      context : .
    working_dir : /home/kinisi
    volumes:
      - ./dags:/home/kinisi/dags
      - ./user:/home/kinisi/udm
      - ./workarea:/home/kinisi/workarea
      - ./logs:/home/kinisi/logs
      - ./certs:/home/kinisi/certs
    command : [ "jupyter", "lab", "--no-browser","--NotebookApp.token=''","--NotebookApp.password=''","--NotebookApp.base_url=/pyed"]
```


## Sample files for /home/kinisi