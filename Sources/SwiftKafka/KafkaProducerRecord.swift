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

import Foundation
import Crdkafka

// API designed to mimic Java API
// https://kafka.apache.org/21/javadoc/index.html?org/apache/kafka/clients/consumer/KafkaConsumer.html

/// The messages that can be sent by a `KafkaProducer`.
public struct KafkaProducerRecord {
    let topic: String
    let value: Data
    let partition: Int32
    let key: Data?
    
    /// Create a new `KafkaProducerRecord` with a `String` value/key
    /// - Parameter topic: The topic where the record will be sent. If this topic doesn't exist the producer will try to create it.
    /// - Parameter value: The message body that will be sent with the record.
    /// - Parameter partition: The topic partition the record will be sent to. If this is not set the partition will be automatically assigned.
    /// - Parameter key: If the partition is not set, records with the same key will be sent to the same partition. Since order is guaranteed within a partition, these records will be read in order they were produced.
    public init(topic: String, value: Data, partition: Int? = nil, key: Data? = nil) {
        self.topic = topic
        self.value = value
        if let recordPartition = partition {
            self.partition = Int32(recordPartition)
        } else {
            self.partition = RD_KAFKA_PARTITION_UA
        }
        self.key = key
    }
    
    /// Create a new `KafkaProducerRecord` with a `Data` value/key
    /// - Parameter topic: The topic where the record will be sent. If this topic doesn't exist the producer will try to create it.
    /// - Parameter value: The message body that will be sent with the record.
    /// - Parameter partition: The topic partition the record will be sent to. If this is not set the partition will be automatically assigned.
    /// - Parameter key: If the partition is not set, records with the same key will be sent to the same partition. Since order is guaranteed within a partition, these records will be read in order they were produced.
    public init(topic: String, value: String, partition: Int? = nil, key: String? = nil) {
        let valueData = Data(value.utf8)
        let keyData = key?.data(using: .utf8)
        self.init(topic: topic, value: valueData, partition: partition, key: keyData)
    }
}
