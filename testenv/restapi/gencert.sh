#!/bin/bash

set -x

HOST=$(hostname)
echo "AU
New South Wales
Grovewood Place
Kinisi
Move
$HOST
not-used" | openssl req -newkey rsa:4096 -nodes -sha256 -keyout domain.key -x509 -days 3650 -out domain.crt


