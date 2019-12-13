#!/usr/bin/env bash
################################################################################
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
################################################################################
source "$(dirname "$0")"/env.sh
SHOW=0
USAGE="
usage: $0 [options]
-h          print this message and exit
-s          show all supported functions
"
while getopts "hs" arg; do
    case "$arg" in
        h)
            printf "%s\\n" "$USAGE"
            exit 2
            ;;
        s)
            SHOW=1
            ;;
        ?)
            printf "ERROR: did not recognize option '%s', please try -h\\n" "$1"
            exit 1
            ;;
    esac
done

if [ $SHOW -eq 1 ]; then
    echo "start_kafka               start kafka"
    echo "stop_kafka                stop kafka"
    echo "create_topic [replication-factor] [partitions] [topic]  example: create_topic 1 1 user -> create topic user with replication-factor is 1 and partitions is 1"
    echo "list_topic                list topic"
    echo "change_topic_partitions   change kafa topic partitions"
    echo "describe_topic            describe kafka topic"
    echo "delete_topic [topic] example: delete_topic user -> delete topic user"
    echo "send_message [topic] [file] example: send_message user_behavior user_behavior.log"
    echo "consume_message [topic]"
    echo "clean_consumer    kill damon concumser"
fi



function check_kafka_dir_set {
    if [[ -z $KAFKA_DIR ]]; then
        echo "Faild to set KAFKA_DIR , you can check the code in env.sh"
        exit 1
    fi
}

function start_zookeeper {
    check_kafka_dir_set
    $KAFKA_DIR/bin/zookeeper-server-start.sh $KAFKA_DIR/config/zookeeper.properties &
}

function stop_zookeeper {
    check_kafka_dir_set
    $KAFKA_DIR/bin/zookeeper-server-stop.sh
}

function start_kafka_server {
    check_kafka_dir_set
    $KAFKA_DIR/bin/kafka-server-start.sh $KAFKA_DIR/config/server.properties &
}

function stop_kafka_server {
    check_kafka_dir_set
    $KAFKA_DIR/bin/kafka-server-stop.sh
}

function check_start {
    # zookeeper outputs the "Node does not exist" bit to stderr
    while [[ $($KAFKA_DIR/bin/zookeeper-shell.sh localhost:2181 get /brokers/ids/0 2>&1) =~ .*Node\ does\ not\ exist.* ]]; do
        echo "Waiting for broker..."
        sleep 1
    done
}

function start_kafka {
    start_zookeeper
    start_kafka_server
    check_start
}

function stop_kafka {
    check_kafka_dir_set
    stop_kafka_server
    stop_zookeeper

    # Terminate Kafka process if it still exists
    PIDS=$(jps -vl | grep -i 'kafka\.Kafka' | grep java | grep -v grep | awk '{print $1}'|| echo "")

    if [ ! -z "$PIDS" ]; then
        kill -s TERM $PIDS || true
    fi

    # Terminate QuorumPeerMain process if it still exists
    PIDS=$(jps -vl | grep java | grep -i QuorumPeerMain | grep -v grep | awk '{print $1}'|| echo "")

    if [ ! -z "$PIDS" ]; then
        kill -s TERM $PIDS || true
    fi
}

function create_topic {
    check_kafka_dir_set
    $KAFKA_DIR/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor $1 --partitions $2 --topic $3 2>&1 >/dev/null
}

function list_topic {
    check_kafka_dir_set
    $KAFKA_DIR/bin/kafka-topics.sh --list --zookeeper localhost:2181
}

function delete_topic {
    check_kafka_dir_set
    $KAFKA_DIR/bin/kafka-topics.sh --delete --zookeeper localhost:2181 --topic $1 2>&1 >/dev/null
    sleep 1
}

function change_topic_partitions {
    check_kafka_dir_set
    $KAFKA_DIR/bin/kafka-topics.sh --zookeeper localhost:2181 --alter --topic $1 --partitions $2 2>&1 >/dev/null
}

function describe_topic {
    check_kafka_dir_set
    $KAFKA_DIR/bin/kafka-topics.sh --zookeeper localhost:2181 --topic $1 --describe
}

function send_message {
    check_kafka_dir_set
    # batch produce to kafka
    $KAFKA_DIR/bin/kafka-console-producer.sh --broker-list localhost:9092 --topic $1 < $2
}

function send_demo_message {
    check_kafka_dir_set
    send_messages_to_kafka '{"user_id": "543462", "item_id":"1715", "category_id": "1464116", "behavior": "pv", "ts": "2017-11-26T01:00:00Z"}' $1
    send_messages_to_kafka '{"user_id": "662867", "item_id":"2244074", "category_id": "1575622", "behavior": "pv", "ts": "2017-11-26T01:00:00Z"}' $1
    send_messages_to_kafka '{"user_id": "561558", "item_id":"3611281", "category_id": "965809", "behavior": "pv", "ts": "2017-11-26T01:00:00Z"}' $1
}

function consume_message {
    check_kafka_dir_set
    $KAFKA_DIR/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic $1 --from-beginning
}

function clean_consumer {
    ps aux | grep "kafka.tools.ConsoleConsumer" | grep -v grep | awk '{print $2}' | xargs sudo kill -9
}

# stop_kafka
# start_kafka
# delete_topic user_behavior
# create_topic 1 1 user_behavior
# send_message user_behavior user_behavior.log