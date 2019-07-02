#!/bin/bash

set -o verbose

if [[ "$TRAVIS_OS_NAME" == "osx" ]]; then
    brew update
    brew cask install homebrew/cask-versions/adoptopenjdk8
    brew install kafka
    brew install librdkafka
    brew services start zookeeper
    brew services start kafka
    sleep 9
else
    apt-get update
    apt-get install -y wget
    wget -qO - https://packages.confluent.io/deb/5.2/archive.key | apt-key add -
    sudo add-apt-repository "deb [arch=amd64] https://packages.confluent.io/deb/5.2 stable main"
    sudo apt-get update && apt-get install confluent-community-2.12 librdkafka-dev
    systemctl start confluent-zookeeper
    systemctl start confluent-kafka
    systemctl start confluent-schema-registry
    sleep 5
fi
