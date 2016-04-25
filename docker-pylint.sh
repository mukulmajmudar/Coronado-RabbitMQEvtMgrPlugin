#!/bin/bash
set -x
docker build -t $USER/coronado-rabbitmqevtmgrplugin .
docker run --rm --entrypoint=pylint $USER/coronado-rabbitmqevtmgrplugin RabbitMQEvtMgrPlugin
