# Airflow-UI

Kinisi Airflow UI is a Web base UI to design airflow DAGS graphically.  It is a containerised application with Notebook and Airflow wbeserver built in to allow quick development of modules.  [See this link for more information and videos](www.kinisi.biz)

This repo is to be used together with the [test environment](https://github.com/chquek/Airflow-Testenv).  This test environment provides the following containers :

- DB2
- MySQL
- Rest API server

# Requirements 

A VM with the following installed ( as root ) :

- apt update
- apt install docker -y
- apt install docker-compose -y
- git clone https://chquek:token@github.com/chquek/Airflow-UI.git
- docker network create kinisi-net  ( this step is necessary because the test environment is on the same cluster )
- usermod -aG docker quekch ( if running as non-root )

## Folders Airflow-UI

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

## Run

- docker-compose up -d

- takes a while for all containers to start up :  nginx , postgres , airflow , designer , notebook

- access via https://hostname


## Restrictions

- Only 5 new DAGs
- Each DAG up to 10 tasks
- Contact admin@kinisi.biz to increase limits
