#!/bin/bash

set -o verbose

if [[ "$TRAVIS_OS_NAME" == "osx" ]]; then
    brew update
    brew cask install java
    brew install kafka
    brew install librdkafka
    zookeeper-server-start /usr/local/etc/kafka/zookeeper.properties
    kafka-server-start /usr/local/etc/kafka/server.properties
    sleep 10
else
    apt-get update
    apt-get install -y wget librdkafka-dev
    wget http://mirror.ox.ac.uk/sites/rsync.apache.org/kafka/2.2.0/kafka_2.12-2.2.0.tgz
    tar -xzf kafka_2.12-2.2.0.tgz
    cd kafka_2.12-2.2.0
    bin/zookeeper-server-start.sh config/zookeeper.properties
    bin/kafka-server-start.sh config/server.properties
    sleep 10
fi

git clone https://github.com/IBM-Swift/Package-Builder.git
./Package-Builder/build-package.sh -projectDir $TRAVIS_BUILD_DIR
