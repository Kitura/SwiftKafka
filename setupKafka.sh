#!/bin/bash

set -o verbose

if [[ "$TRAVIS_OS_NAME" == "osx" ]]; then
    brew update > /dev/null
    brew cask install java
    brew install kafka
    zookeeper-server-start /usr/local/etc/kafka/zookeeper.properties
    kafka-server-start /usr/local/etc/kafka/server.properties
    sleep 10
else
    apt-get install -y wget
    wget http://mirror.ox.ac.uk/sites/rsync.apache.org/kafka/2.2.0/kafka_2.12-2.2.0.tgz
    tar -xzf kafka_2.12-2.2.0.tgz
    cd kafka_2.12-2.2.0
    bin/zookeeper-server-start.sh config/zookeeper.properties
    bin/kafka-server-start.sh config/server.properties
fi
