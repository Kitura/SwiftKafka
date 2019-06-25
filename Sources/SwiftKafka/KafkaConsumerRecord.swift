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

// https://kafka.apache.org/21/javadoc/index.html?org/apache/kafka/clients/consumer/KafkaConsumer.html

/// The message returned by a `KafkaConsumer` polling a broker.
public struct KafkaConsumerRecord: Codable {
    /// The message value if it can be utf8 decoded to a String.
    public let value: String?
    /// The message value as raw data.
    public let valueData: Data
    /// The message key if it can be utf8 decoded to a String.
    public let key: String?
    /// The message key as raw data.
    public let keyData: Data?
    /// The message offset.
    public let offset: Int
    /// The topic that the message was consumed from.
    public let topic: String
    /// The partition that the message was consumed from.
    public let partition: Int
    
    init?(messagePointer: UnsafeMutablePointer<rd_kafka_message_t>) {
        let rdMessage = messagePointer.pointee
        // Extract message value
        if let payloadPointer = rdMessage.payload {
            let valueData = Data(bytes: payloadPointer.assumingMemoryBound(to: UInt8.self), count: rdMessage.len)
            self.valueData =  valueData
            self.value = String(data: valueData, encoding: .utf8)
        } else {
            return nil
        }
        
        // extract message key
        if let keyPointer = rdMessage.key {
            let keyData = Data(bytes: keyPointer.assumingMemoryBound(to: UInt8.self), count: rdMessage.key_len)
            self.keyData =  keyData
            self.key = String(data: keyData, encoding: .utf8)
        } else {
            self.key = nil
            self.keyData = nil
        }
        
        // set message offset
        self.offset = Int(rdMessage.offset)
        
        // set topic
        guard let topic = String(validatingUTF8: rd_kafka_topic_name(rdMessage.rkt)) else {
            return nil
        }
        self.topic = topic
        
        // set message partition
        self.partition = Int(rdMessage.partition)
    }
}
