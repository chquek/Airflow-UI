#!/bin/bash

msg "--- Generate certificate and private key for https ---"
HOST=$(hostname)
echo "AU
First Farm
St Leonard
Kinisi
Airflow-UI
$HOST
not-used" | openssl req -newkey rsa:4096 -nodes -sha256 -keyout privkey1.pem -x509 -days 3650 -out fullchain1.pem
