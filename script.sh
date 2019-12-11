#!/bin/bash

confluent local start
confluent local status
confluent local load source_autorest -- -d /Users/fanrui/Kafka/confluent-5.3.1/source_autorest.json
confluent local status connectors

#elasticsearch &
#P1=$!

#kibana &
#P2=$!

curl -X GET 'http://localhost:9200/_cat/indices?v'
open -a Terminal.app echotest.sh
#sshpass -p 'maria_dev' ssh maria_dev@localhost -p 2222


#xterm -e python /Users/fanrui/Kafka/es-sink.py
sshpass -p 'maria_dev' ssh maria_dev@localhost -p 2222 < /Users/fanrui/Kafka/sparksubmit.sh
