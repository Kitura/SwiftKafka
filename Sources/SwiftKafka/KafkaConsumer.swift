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

import Crdkafka
import Foundation
import Dispatch

/**
 A `KafkaClient` for consuming messages on a topic from a broker. 
 ### Usage Example:
 ```swift
 do {
     let config = KafkaConfig()
     config.groupId = "Kitura"
     config.autoOffsetReset = .beginning
     let consumer = try KafkaConsumer(config: config)
     consumer.connect(brokers: "localhost:9092")
     try consumer.subscribe(topics: ["test"])
     let records = consumer.poll()
     print(records)
 } catch {
     print("Error creating consumer: \(error)")
 }
 ```
 */
public class KafkaConsumer: KafkaClient {
    
    // A pointer to a list of the topics this consumer is subscribed to.
    let topicsPointer: UnsafeMutablePointer<rd_kafka_topic_partition_list_t>
    // Becomes true if the consumer is closed early via the close() method.
    var closed = false
    
    /// Create a new `KafkaConsumer`.
    /// If a groupID is not provided in the config, a random UUID will be used.
    /// - Parameter config: The `KafkaConfig` that will configure your Kafka consumer.
    /// - Throws: A `KafkaError` if the provided `KafkaConfig` is invalid.
    public init(config: KafkaConfig = KafkaConfig()) throws {
        self.topicsPointer = rd_kafka_topic_partition_list_new(1)
        try super.init(clientType: .consumer, config: config)
        let setResult = rd_kafka_poll_set_consumer(kafkaHandle)
        guard Int(setResult.rawValue) == 0 else {
            throw KafkaError(rawValue: Int(setResult.rawValue))
        }
    }
    
    /// :nodoc:
    deinit {
        // Close the consumer if close() hasn't already been called
        if !closed {
            rd_kafka_consumer_close(kafkaHandle)
            rd_kafka_topic_partition_list_destroy(topicsPointer)
        }
    }
    
    /// Subscribe to one or more topics. The consumer will be assigned partitions based on its consumer group.
    ///  The subscribe() method is asynchronouswhich and will returns immediately. Background threads will (re)join the group, wait for group rebalance, issue any registered rebalance_cb, assign() the assigned partitions, and then start fetching messages. This cycle may take up to session.timeout.ms * 2 or more to complete.
    /// - Parameter topics: An array of Kafka topics to subscribe to.
    /// - Throws: A `KafkaError` if the consumer fails to subscribe the the topic.
    public func subscribe(topics: [String]) throws {
        for topic in topics {
            // Add the topics to the `rd_kafka_topic_partition_list_t` with unassigned partitions(RD_KAFKA_PARTITION_UA).
            // This leaves the partition assignment up to the broker using consumer groups.
            rd_kafka_topic_partition_list_add(topicsPointer, topic, RD_KAFKA_PARTITION_UA)
        }
        let result = rd_kafka_subscribe(kafkaHandle, topicsPointer)
        guard result.rawValue == 0 else {
            throw KafkaError(rawValue: Int(result.rawValue))
        }
    }
    
    /// Assign this consumer to a single topic with a specified partition to consume from. 
    /// The offset to begin consuming from can also be specified, otherwise the consumer default to the end of the current messages.
    /// - Parameter topic: The Kafka topic to subscribe to.
    /// - Parameter partition: The topic partition to consume from.
    /// - Parameter offset: The topic offset to begin consuming from. Defaults to -1 representing the end of current messages.
    /// - Throws: A `KafkaError` if the consumer fails to subscribe the the topic.
    public func assign(topic: String, partition: Int, offset: Int = Int(RD_KAFKA_OFFSET_END)) throws {
        if let partitionPointer = rd_kafka_topic_partition_list_add(topicsPointer, topic, Int32(partition)) {
            partitionPointer.pointee.offset = Int64(offset)
            let result = rd_kafka_assign(kafkaHandle, topicsPointer)
            guard result.rawValue == 0 else {
                throw KafkaError(rawValue: Int(result.rawValue))
            }
        }
    }
    
    /// Consume messages from the topic you are subscribed to.
    /// The messages will be consumed from your last call to poll.
    /// This function will block for at most timeout seconds.
    /// - Parameter timeout: The maximum `TimeInterval` in seconds that this call will block for while consuming messages.
    /// - returns: An array of `KafkaConsumerRecord` that have been consumed.
    public func poll(timeout: TimeInterval = 1) throws -> [KafkaConsumerRecord] {
        guard !closed else {
            throw KafkaError(description: "Consumer connection has been closed")
        }
        let startDate = Date()
        var records = [KafkaConsumerRecord]()
        // Poll Kafka in a loop until either an error is thrown or the timeout is reached. 
        while(Date() < startDate + timeout) {
            // rd_kafka_consumer_poll returns a single kafka record and blocks at most timeout/10 seconds.
            if let msgPointer = rd_kafka_consumer_poll(kafkaHandle, Int32(timeout * 100)) {
                // err = 0 on success or -191 on no more messages
                if msgPointer.pointee.err == RD_KAFKA_RESP_ERR__PARTITION_EOF {
                    continue
                } else if msgPointer.pointee.err.rawValue != 0 {
                    throw KafkaError(rawValue: Int(msgPointer.pointee.err.rawValue))
                } else {
                    if let record = KafkaConsumerRecord(messagePointer: msgPointer) {
                        records.append(record)
                    }
                }
                rd_kafka_message_destroy(msgPointer)
            } 
            // Poll may not always return a message.
            // It might instruct the consumer to rebalance which causes the flow to end here so we continue.
        }
        return records
    }
    
    /// Commit the offsets for the current partition assignment on the broker.  
    /// This marks the records since the last poll as processed so they will not be reassigned during a rebalance.
    /// If you are using `commitSync()`, `enableAutoCommit` on `KafkaConfig` should be set to false.
    public func commitSync() throws {
        let result = rd_kafka_commit(kafkaHandle, nil, 0)  
        guard result.rawValue == 0 else {
            throw KafkaError(rawValue: Int(result.rawValue))
        }
    }
    
    /// Close down the `KafkaConsumer`.
    /// This call will block until the consumer has revoked its assignment.
    /// The maximum blocking time is roughly limited to session.timeout.ms.
    /// If you don't call this function, the consumer will be closed when it is deinitialized.
    public func close() throws {
        let response = rd_kafka_consumer_close(kafkaHandle)
        guard Int(response.rawValue) == 0 else {
            throw KafkaError(rawValue: Int(response.rawValue))
        }
        rd_kafka_topic_partition_list_destroy(topicsPointer)
        closed = true
    }
}
