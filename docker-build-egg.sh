#!/bin/bash
set -x
docker build -t $USER/coronado-rabbitmqevtmgrplugin .
mkdir -p dist
docker run --rm \
    -e USERID=$EUID \
    -v `pwd`/dist:/root/RabbitMQEvtMgrPlugin/dist \
    $USER/coronado-rabbitmqevtmgrplugin
