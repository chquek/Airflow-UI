# Airflow-UI

Kinisi Airflow UI


# Requirements 

A VM with the following installed ( as root ) :

- apt update
- apt install docker -y
- apt install docker-compose -y
- git clone https://chquek:token@github.com/chquek/Airflow-UI.git

Cd to the Airflow-UI folder

- cp DONOTREMOVE/menu.py /usr/local/bin
- docker network create kinisi-net  ( this step is necessary because the test environment is on the same cluster )

# This step only if require to run docker as non-root.  Run the following as root

- groupadd docker ( if does not exist in /etc/group )
- usermod -aG docker quekch

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


## Sample files for /home/kinisi