<p align="center">
<a href="http://kitura.io/">
<img src="https://raw.githubusercontent.com/IBM-Swift/Kitura/master/Sources/Kitura/resources/kitura-bird.svg?sanitize=true" height="100" alt="Kitura">
</a>
</p>


<p align="center">
<a href="https://ibm-swift.github.io/SwiftKafka/index.html">
<img src="https://img.shields.io/badge/apidoc-SwiftKafka-1FBCE4.svg?style=flat" alt="APIDoc">
</a>
<a href="https://travis-ci.org/IBM-Swift/SwiftKafka">
<img src="https://travis-ci.org/IBM-Swift/SwiftKafka.svg?branch=master" alt="Build Status - Master">
</a>
<img src="https://img.shields.io/badge/os-macOS-green.svg?style=flat" alt="macOS">
<img src="https://img.shields.io/badge/os-linux-green.svg?style=flat" alt="Linux">
<img src="https://img.shields.io/badge/license-Apache2-blue.svg?style=flat" alt="Apache 2">
<a href="http://swift-at-ibm-slack.mybluemix.net/">
<img src="http://swift-at-ibm-slack.mybluemix.net/badge.svg" alt="Slack Status">
</a>
</p>

# SwiftKafka

A swift implementation of [Kafka](https://kafka.apache.org/) for producing and consuming from event streams.

This works by wrapping the [librdkafka](https://github.com/edenhill/librdkafka) C library.


## Swift version

The latest version of SwiftKafka requires **Swift 5.0** or later. You can download this version of the Swift binaries by following this [link](https://swift.org/download/).

## Usage

### Swift Package Manager

#### Add dependencies
Add the `SwiftKafka` package to the dependencies within your application’s `Package.swift` file. Substitute `"x.x.x"` with the latest `SwiftKafka` [release](https://github.com/IBM-Swift/Swift-Kafka/releases).
```swift
.package(url: "https://github.com/IBM-Swift/SwiftKafka.git", from: "x.x.x")
```
Add `SwiftKafka` to your target's dependencies:
```swift
.target(name: "example", dependencies: ["SwiftKafka"]),
```
#### Import package
```swift
import SwiftKafka
```

## Getting Started

To use SwiftKafka you will need to install the `librdkafka` package:

### macOS
```
brew install librdkafka
```

### Linux
Install librdkafka from the Confluent APT repositories - [see instructions here](https://docs.confluent.io/current/installation/installing_cp/deb-ubuntu.html#get-the-software) (following steps 1 and 2 to add the Confluent package signing key and apt repository), and then install librdkafka:
```
sudo apt install librdkafka
```

#### Running a Kafka instance locally
To experiment locally, you can set up your own Kafka server to produce/consume from.

On macOS you can follow this guide on [Kafka Installation using Homebrew](https://medium.com/@Ankitthakur/apache-kafka-installation-on-mac-using-homebrew-a367cdefd273) to run a local server.

On Linux, you can follow this guide for [a manual install on Ubuntu](https://docs.confluent.io/current/installation/installing_cp/deb-ubuntu.html).

### KafkaConfig

The `KafkaConfig` class contains your configuration settings for a `KafkaConsumer`/`KafkaProducer`.  

The class is initialized with default values which can then be changed using the helper functions.
For example, to enable all logging you would set the debug variable:
```swift
let config = KafkaConfig()
config.debug = [.all]
```

Alternatively, you can access the configuration dictionary directly on the `KafkaConfig` object:

```swift
let config = KafkaConfig()
config["debug"] = "all"
```
The list of configuration keys and descriptions can be found in the librdkafka [CONFIGURATION.md](https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md).

When you pass this class to a producer/consumer, a copy is made so further changes to the instance will not affect existing configurations.

### KafkaProducer:

The `KafkaProducer` class produces messages to a Kafka server.

You can initialize a `KafkaProducer` using a `KafkaConfig` instance or with the default configuration.

The producer sends a `KafkaProducerRecord` with the following fields:

- topic: The topic where the record will be sent. If this topic doesn't exist the producer will try to create it.
- value: The message body that will be sent with the record.
- partition: The topic partition the record will be sent to. If this is not set the partition will be automatically assigned.
- key: If the partition is not set, records with the same key will be sent to the same partition. Since order is guaranteed within a partition, these records will be read in order they were produced.

The `send()` function is asynchronous. The result is returned in a callback which contains a `KafkaConsumerRecord` on success or a `KafkaError` on failure.

The following example produces a message with the value "Hello World" to a "test" topic of a Kafka server running on localhost.

```swift
do {
    let producer = try KafkaProducer()
    guard producer.connect(brokers: "localhost:9092") == 1 else {
        throw KafkaError(rawValue: 8)
    }
    producer.send(producerRecord: KafkaProducerRecord(topic: "test", value: "Hello world", key: "Key")) { result in
        switch result {
        case .success(let message):
            print("Message at offset \(message.offset) successfully sent")
        case .failure(let error):
            print("Error producing: \(error)")
        }
    }
} catch {
    print("Error creating producer: \(error)")
}
```

### KafkaConsumer:

The `KafkaConsumer` class consumes messages from a Kafka server.

You can initialize a `KafkaConsumer` using a `KafkaConfig` instance or with the default configuration.

You can then subscribe to topics using `subscribe()`.
This will distribute the topic partitions evenly between consumers with the same group id.
If you do not set a group id, a random UUID will be used.

Alternatively to can use `assign()` to manually set the partition and offset for the consumer.

Both `subscribe()` and `assign()` are asynchronous and will return immediately, however they may take up to sessionTimeoutMs (Default 10 seconds) * 2 before the consumer completely connects.

To consume messages from Kafka you call `poll(timeout:)`. This will poll Kafka, blocking for `timeout` seconds. When it completes, it returns an array of `KafkaConsumerRecord` with the following fields:

- value: The message value if it can be UTF8 decoded to a String.
- valueData: The message value as raw data.
- key: The message key if it can be utf8 decoded to a String.
- keyData: The message key as raw data.
- offset: The message offset.
- topic: The topic that the message was consumed from.
- partition: The partition that the message was consumed from.

When you have finished consuming, you can call `close()` to close the connection and unassigns the consumer.
The unassigned partitions will then be rebalanced between other consumers in the group.
If  `close()` is not called, the consumer will be closed when the class is deallocated.

The following example consumes and print all unread messages from the "test" topic of the Kafka server.

```swift
do {
    let config = KafkaConfig()
    config.groupId = "Kitura"
    config.autoOffsetReset = .beginning
    let consumer = try KafkaConsumer(config: config)
    guard consumer.connect(brokers: "localhost:9092") == 1 else {
        throw KafkaError(rawValue: 8)
    }
    try consumer.subscribe(topics: ["test"])
    while(true) {
        let records = try consumer.poll()
        print(records)
    }
} catch {
    print("Error creating consumer: \(error)")
}
```

## API Documentation
For more information visit our [API reference](https://ibm-swift.github.io/Swift-Kafka/index.html).

## Community

We love to talk server-side Swift, and Kitura. Join our [Slack](http://swift-at-ibm-slack.mybluemix.net/) to meet the team!

## License
This library is licensed under Apache 2.0. Full license text is available in [LICENSE](https://github.com/IBM-Swift/Swift-Kafka/blob/master/LICENSE).
