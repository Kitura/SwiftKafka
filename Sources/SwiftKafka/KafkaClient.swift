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
import Logging

/// A class for clients that will connect to a Kafka server.  
/// This can either be a `KafkaConsumer` or a `KafkaProducer`.
public class KafkaClient {
    
    // Size reserved for a string in C
    static let stringSize = 1024

    // Pointer to the C handler for Kafka
    let kafkaHandle: OpaquePointer
    
    static let logger: Logger = Logger(label: "KafkaClientLogger")
    
    // Is this client a Kafka consumer or producer
    enum ClientType {
        case consumer, producer
    }
    
    // Initialise a KafkaClient of the provided type
    init(clientType: ClientType, config: KafkaConfig) throws {
        let rdType = clientType == .producer ? RD_KAFKA_PRODUCER : RD_KAFKA_CONSUMER
        let errstr = UnsafeMutablePointer<CChar>.allocate(capacity: KafkaConsumer.stringSize)
        defer { errstr.deallocate() }
        let configPointer = config.getPointerDuplicate()
        // If consumer has not set a groupID, use a random UUID.
        if clientType == .consumer && config.groupId == nil {
            let uuid = UUID().uuidString
            KafkaClient.logger.debug("Unique consumer group created with UUID: \(uuid)")
            rd_kafka_conf_set(configPointer, "group.id", uuid, nil, 0)
        }
        // Create a new `rd_kafka_t` object using the config and return an opaque pointer to that object. 
        guard let handle = rd_kafka_new(rdType, configPointer, errstr, KafkaClient.stringSize) else {
            throw KafkaError(description: String(cString: errstr))
        }
        self.kafkaHandle = handle
    }
    
    /// :nodoc:
    deinit {
        // Deallocate the memory for the rd_kafka_t object.
        rd_kafka_destroy(kafkaHandle)
    }
    
    /** 
     Connect as a client to the servers (Kafka broker) at the provided addresses.
     ### Usage Example:
     ```swift
     let producer = try? KafkaProducer()
     let connectionCount = producer?.connect(brokers: "localhost:9092, localhost:9093")
     guard connectionCount == 2 else {
        print("Failed to connect to brokers")
     }
     ```
     - Parameter brokers: The brokers urls as a comma seperated String.
     - Returns: The number of brokers that were successfully connected.
     */
    @discardableResult
     public func connect(brokers: String) -> Int {
        return Int(rd_kafka_brokers_add(kafkaHandle, brokers))
    }
    
    /** 
     Connect as a client to the servers (Kafka broker) at the provided addresses.
     ### Usage Example:
     ```swift
     let producer = try? KafkaProducer()
     let connectionCount = producer?.connect(brokers: ["localhost:9092", "localhost:9093"])
     guard connectionCount == 2 else {
        print("Failed to connect to brokers")
     }
     ```
     - Parameter brokers: The brokers urls as an array of Strings.
     - Returns: The number of brokers that were successfully connected.
     */
    @discardableResult
    public func connect(brokers: [String]) -> Int {
        return Int(rd_kafka_brokers_add(kafkaHandle, brokers.joined(separator: ",")))
    }
    
    /** 
     Connect as a client to the servers (Kafka broker) at the provided addresses.
     ### Usage Example:
     ```swift
     let producer = try? KafkaProducer()
     let connectionCount = producer?.connect(brokers: ["localhost": 9092, "localhost": 9093])
     guard connectionCount == 2 else {
        print("Failed to connect to brokers")
     }
     ```
     - Parameter brokers: The brokers urls as a dictionary of [host: port].
     - Returns: The number of brokers that were successfully connected.
     */
    @discardableResult
    public func connect(brokers: [String: Int]) -> Int {
        let brokersString: String = brokers.map { arg in
            let (key, value) = arg
            return key + ":" + String(value)
        }.joined(separator: ",")
        return Int(rd_kafka_brokers_add(kafkaHandle, brokersString))
    }
}



