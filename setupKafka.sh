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
    if [[ ! -z "$DOCKER_IMAGE" ]]; then
        #
        # Taken from Confluent Quickstart (with Docker) guide:
        # https://docs.confluent.io/current/quickstart/cos-docker-quickstart.html#cos-docker-quickstart
        # Start Kafka components in containers. Communication will be via Docker bridge network.
        #
        git clone https://github.com/confluentinc/cp-docker-images
        cd cp-docker-images
        git checkout 5.2.2-post
        cd examples/cp-all-in-one/
        docker-compose up -d --build
        docker-compose ps
        # List active networks - confirm name of kafka bridge network (cp-all-in-one_default)
        docker network ls
    else
        #
        # Taken from Confluent Install guide:
        # https://docs.confluent.io/current/installation/installing_cp/deb-ubuntu.html#systemd-ubuntu-debian-install
        #
        sudo apt-get update
        sudo apt-get install -y wget
        wget -qO - https://packages.confluent.io/deb/5.2/archive.key | sudo apt-key add -
        sudo add-apt-repository "deb [arch=amd64] https://packages.confluent.io/deb/5.2 stable main"
        sudo apt-get update
        sudo apt-get install -y confluent-community-2.12 librdkafka-dev
        sudo systemctl start confluent-zookeeper
        sudo systemctl start confluent-kafka
        sudo systemctl start confluent-schema-registry
        sleep 5
    fi
fi
