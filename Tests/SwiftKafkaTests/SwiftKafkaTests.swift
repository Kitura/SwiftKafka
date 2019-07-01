/*
 * Copyright IBM Corporation 2019
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import XCTest
@testable import SwiftKafka
import Crdkafka

final class SwiftKafkaTests: XCTestCase {
    static var allTests = [
        ("testProduceConsume", testProduceConsume),
        ("testAssign", testAssign),
        ("testConfig", testConfig),
        ("testCommitSync", testCommitSync),
        ("testProduceCallback", testProduceCallback),
    ]
    
    // Homebrew instructions for mac https://medium.com/@Ankitthakur/apache-kafka-installation-on-mac-using-homebrew-a367cdefd273
    
    // These tests require a Zookeeper and Kafka server to be running
    // zookeeper-server-start /usr/local/etc/kafka/zookeeper.properties
    // kafka-server-start /usr/local/etc/kafka/server.properties
    
    func testProduceConsume() {
        do {
            let config = KafkaConfig()
            // Use IPv4 for localhost
            config.brokerAddressFamily = .v4
            let producer = try KafkaProducer(config: config)
            let consumer = try KafkaConsumer(config: config)
            guard consumer.connect(brokers: "localhost:9092") == 1,
                producer.connect(brokers: "localhost:9092") == 1 
                else {
                    return XCTFail("Failed to connect to brokers. Ensure Kafka server is running.")
            }
            try consumer.subscribe(topics: ["test", "test1"])
            // Poll to set starting offset at end of messages
            let _ = try consumer.poll()
            producer.send(producerRecord: KafkaProducerRecord(topic: "test", value: "Hello world", key: "Key"))
            producer.send(producerRecord: KafkaProducerRecord(topic: "test1", value: Data("Hello Kitura".utf8), key: Data("Key".utf8)))
            // Give time for produces message to be sent and updated on the kafka service
            var records = [KafkaConsumerRecord]()
            for _ in 0..<20 {
                let polledRecords = try consumer.poll()
                records.append(contentsOf: polledRecords)
                if records.count >= 2 {
                    break
                }
            }
            try consumer.close()
            XCTAssertGreaterThan(records.count, 1)
        } catch {
            return XCTFail((error as? KafkaError)?.description ?? "")
        }
    }
    
    func testConfig() {
        let config = KafkaConfig()
        config.clientID = "test"
        XCTAssertEqual(config.dictionary["client.id"], "test")
    }
    
    func testAssign() {
        do {
            let config = KafkaConfig()
            config.brokerAddressFamily = .v4
            let producer = try KafkaProducer(config: config)
            let consumer = try KafkaConsumer(config: config)
            guard consumer.connect(brokers: "localhost:9092") == 1,
                producer.connect(brokers: "localhost:9092") == 1 
            else {
                return XCTFail("Failed to connect to brokers. Ensure Kafka server is running.")
            }
            try consumer.assign(topic: "test2", partition: 0)
            // Wait for consumer to be assigned at latest message
            sleep(1)
            let _ = try consumer.poll()
            producer.send(producerRecord: KafkaProducerRecord(topic: "test2", value: "Hello Assign", partition: 0, key: "123"))
            producer.send(producerRecord: KafkaProducerRecord(topic: "test2", value: "Hello Assign", partition: 1, key: "123"))
            // Give time for produces message to be sent and updated on the kafka service
            var records = [KafkaConsumerRecord]()
            for _ in 0..<20 {
                let polledRecords = try consumer.poll()
                records.append(contentsOf: polledRecords)
                if records.count >= 1 {
                    break
                }
            }
            // Consumer should only consume one message from partition 0
            XCTAssertEqual(records.count, 1)
        } catch {
            return XCTFail((error as? KafkaError)?.description ?? "")
        }
    }
    
    func testCommitSync() {
        do {
            let config = KafkaConfig()
            config.brokerAddressFamily = .v4
            let producer = try KafkaProducer(config: config)
            config.enableAutoCommit = false
            config.groupId = "testCommitSync"
            let consumer = try KafkaConsumer(config: config)
            let brokersCount = consumer.connect(brokers: "localhost:9092")
            XCTAssertEqual(brokersCount, 1)
            let producerBrokersCount = producer.connect(brokers: "localhost:9092")
            XCTAssertEqual(producerBrokersCount, 1)
            try consumer.subscribe(topics: ["test3"])
            sleep(1)
            // Poll to set consumer to end of messages
            let _ = try consumer.poll()
            for i in 0..<10 {
                producer.send(producerRecord: KafkaProducerRecord(topic: "test3", value: "message \(i)"))
            }
            // Give time for produces message to be sent and updated on the kafka service
            sleep(1)
            var records = [KafkaConsumerRecord]()
            for _ in 0..<20 {
                let polledRecords = try consumer.poll()
                records.append(contentsOf: polledRecords)
                if records.count >= 10 {
                    break
                }
            }
            try consumer.commitSync()
            XCTAssertEqual(records.count, 10)
        } catch {
            return XCTFail((error as? KafkaError)?.description ?? "")
        }
    }
    
    func testProduceCallback() {
        do {
            let produceExpectation = expectation(description: "message callback is produced")
            let config = KafkaConfig()
            config.brokerAddressFamily = .v4
            let producer = try KafkaProducer(config: config)
            let producerBrokersCount = producer.connect(brokers: "localhost:9092")
            XCTAssertGreaterThan(producerBrokersCount, 0)
            producer.send(producerRecord: KafkaProducerRecord(topic: "test4", value: "Hello world", key: "Key")) { result in
                switch result {
                case .success(_):
                    produceExpectation.fulfill()
                case .failure(let error):
                    print("Error producing: \(error)")
                }
            }
        } catch {
            XCTFail("Failed with error \(error.localizedDescription)")
        }
        waitForExpectations(timeout: 10) { error in
            // blocks test until request completes
            XCTAssertNil(error)
        }
    }
}
