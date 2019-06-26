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
import Dispatch
import Logging

/**
 A threadsafe `KafkaClient` for producing messages to a topic on a broker. 
 ### Usage Example:
 ```swift
 do {
     let producer = try KafkaProducer()
     producer.connect(brokers: "localhost:9092")
     producer.send(producerRecord: KafkaProducerRecord(topic: "test", value: "Hello World")) { result in
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
 */
public class KafkaProducer: KafkaClient {
    // You must access topicPointers through a semaphore for thread safety
    let topicSemaphore = DispatchSemaphore(value: 1)
    var topicPointers = [String: OpaquePointer]()
    
    // You must access callbackPointers through a semaphore for thread safety
    static let callbackSemaphore = DispatchSemaphore(value: 1)
    
    // This Dictionary maps the KafkaHandle (OpaquePointer) of a KafkaProducer to a dictionary of idPointers (UnsafeMutableRawPointer) keyed against messageCallbacks ((Result<KafkaConsumerRecord, KafkaError>) -> Void) values.
    // Since C closures cannot capture context, this is used to get the users callback when the call send from the kafka handle returned by rd_kafka_conf_set_dr_msg_cb.
    static var kafkaHandleToMessageCallback = [OpaquePointer: [UnsafeMutableRawPointer: (Result<KafkaConsumerRecord, KafkaError>) -> Void]]()
    let callbackTimer: DispatchSourceTimer
    
    
    /// Create a new `KafkaProducer` for sending messages to Kafka.
    /// - Parameter config: The `KafkaConfig` that will configure your Kafka producer.
    /// - Parameter pollInterval: The time in seconds that the producer will poll for message callbacks.
    public init(config: KafkaConfig = KafkaConfig(), pollInterval: TimeInterval = 1) throws {
        self.callbackTimer = DispatchSource.makeTimerSource()
        try super.init(clientType: .producer, config: config)
        self.timerStart(pollInterval: pollInterval)
        // Create an empty dictionary for this producers messageCallbacks
        KafkaProducer.kafkaHandleToMessageCallback[self.kafkaHandle]?.forEach { (key, _) in 
            key.deallocate()
            KafkaProducer.kafkaHandleToMessageCallback[self.kafkaHandle]?[key] = nil
        }
    }
    
    /// :nodoc:
    deinit {
        // Wait for outstanding messages to be send and their callbacks to be called.
        rd_kafka_flush(kafkaHandle, 10000 /* wait for max 10 seconds */)
        for (_, topic) in topicPointers {
            rd_kafka_topic_destroy(topic)
        }
        // Cancel the poll timer
        callbackTimer.setEventHandler {}
        callbackTimer.cancel()
        // remove this producer from the Dictionary of KafkaHandles to callbacks
        KafkaProducer.callbackSemaphore.wait()
        KafkaProducer.kafkaHandleToMessageCallback[self.kafkaHandle] = nil
        KafkaProducer.callbackSemaphore.signal()
    }
    
    /// Send a `KafkaProducerRecord` to the broker.
    /// The result of sending the message will be returned in the messageCallback.
    /// - Parameter producerRecord: The `KafkaProducerRecord` that will be sent to Kafka.
    /// - Parameter messageCallback: The callback that will be called with the result of trying to send a message.
    public func send(producerRecord: KafkaProducerRecord, messageCallback: ((Result<KafkaConsumerRecord, KafkaError>) -> Void)? = nil) {
        
        // Get the topic pointer for the KafkaProducerRecord topic or create a new topic if one doesn't exist 
        let topicPointer: OpaquePointer?
        topicSemaphore.wait()
        if let topic = topicPointers[producerRecord.topic] {
            topicPointer = topic
        } else {
            topicPointer = rd_kafka_topic_new(kafkaHandle, producerRecord.topic, nil)
            topicPointers[producerRecord.topic] = topicPointer
        }
        topicSemaphore.signal()
        let keyBytes: [UInt8]?
        let keyBytesCount: Int
        if let key = producerRecord.key {
            keyBytes = [UInt8](key)
            keyBytesCount = key.count
        } else {
            keyBytes = nil
            keyBytesCount = 0
        }
        // Allocate a byte of memory to act as an identifer for the message callback
        let idPointer = UnsafeMutableRawPointer.allocate(byteCount: 1, alignment: 0)
        let responseCode = rd_kafka_produce(topicPointer,
                                            producerRecord.partition,
                                            RD_KAFKA_MSG_F_COPY,
                                            UnsafeMutablePointer<UInt8>(mutating: [UInt8](producerRecord.value)),
                                            producerRecord.value.count,
                                            keyBytes,
                                            keyBytesCount,
                                            idPointer)
        if responseCode != 0 {
            if let callback = messageCallback {
                callback(.failure(KafkaError(rawValue: Int(responseCode))))
            } 
        }
        KafkaProducer.callbackSemaphore.wait()
        // Set the message callback for the idPointer for this kafka producer.
        KafkaProducer.kafkaHandleToMessageCallback[self.kafkaHandle]?[idPointer] = messageCallback
        KafkaProducer.callbackSemaphore.signal()
        rd_kafka_poll(kafkaHandle, 0)
    }
    
    // This timer runs Poll at regular intervans to collect message callbacks.
    private func timerStart(pollInterval: TimeInterval) {
        callbackTimer.schedule(deadline: .now(), repeating: pollInterval, leeway: DispatchTimeInterval.milliseconds(Int(pollInterval * 100)))
        callbackTimer.setEventHandler(handler: { [weak self] in
            if let strongSelf = self {
                rd_kafka_poll(strongSelf.kafkaHandle, 0)
            }
        })
        callbackTimer.resume()
    }
}
