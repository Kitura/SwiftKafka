#!/bin/bash

set -o verbose

if [[ "$TRAVIS_OS_NAME" == "osx" ]]; then
    brew update
    brew cask install java
    brew install kafka
    brew install librdkafka
    brew services start zookeeper
    brew services start kafka
    sleep 9
else
    apt-get update
    apt-get install -y wget librdkafka-dev
    wget http://mirror.ox.ac.uk/sites/rsync.apache.org/kafka/2.2.0/kafka_2.12-2.2.0.tgz -O kafka.tgz
    mkdir -p kafka && tar xzf kafka.tgz -C kafka --strip-components 1
    nohup bash -c "cd kafka && bin/zookeeper-server-start.sh config/zookeeper.properties &"
    nohup bash -c "cd kafka && bin/kafka-server-start.sh config/server.properties &"
    sleep 9
fi
