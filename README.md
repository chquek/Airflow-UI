# Airflow-UI

## Introduction

Kinisi Airflow UI is a Web base UI to design airflow DAGs graphically.  It is a containerised application with integrated Notebook and Airflow wbeserver built in to allow quick development of modules.  [See this link for more information and videos](https://www.kinisi.biz)

## Requirements 

A VM with the following installed ( as root ) :

- 4 CPUs and 8GB RAM
- apt update
- apt install docker -y
- apt install docker-compose -y
- docker network create kinisi-net  ( this step is necessary because the test environment is on the same cluster )
- usermod -aG docker userX ( if running as non-root )

## Usage

- amend and source the envfile.  This file set the login id/pw to the UI default to admin/admin.

- docker-compose up -d

- takes a while for all containers to start up :  nginx , postgres , airflow , designer , notebook

- access via https://hostname

## Generated Folders

At the first start up, the following folders will be created automatically with some sample DAGs.

Folder | Description | Mounted to 
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

## Airflow credentials

The airflow id is admin/admin.  It is built into the image.  You will need this is need to do airflow admin task such as adding connections.

## Sample Databases

This repo is to be used together with the [test environment](https://github.com/chquek/Airflow-Testenv).  Some of the demo DAGs require connectivity to databases and REST server.  This test environment provides the following containers :

- DB2 ( SSL and non-SSL )
- MySQL ( SSL and non-SSL )
- Rest API server
- Kafka
