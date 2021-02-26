//
// Created by Kacper Kawecki on 25/02/2021.
//

import Foundation
import Crdkafka
import Ckafkahelper

public class KafkaAdmin {
    let kafkaHandle: OpaquePointer
    
    private struct ErrorMapping: Error {
        let kafkaError: KafkaError
        let topicName: String
    }

    public struct KafkaTopicSpecificationError: Error {
        let kafkaError: KafkaError
        let topicSpecification: KafkaTopicSpecification
    }

    init(client: KafkaClient) {
        self.kafkaHandle = client.kafkaHandle
    }

    /// Create topics with basic setup
    ///
    /// - Parameters:
    ///   - topicNames: Array of topic names of which will be created
    ///   - numPartitions: Numer of partitions per topic. Default is 1
    ///   - replicationFactor: Topic replication factor. Set -1 to use cluster value. Default is -1
    ///   - timeout: Time in ms for how long it should wait for replay from kafka before throwing error. Default is 5000 (5s)
    /// - Returns: Array of result with KafkaTopicSpecification or KafkaTopicSpecificationError
    /// - Throws: KafkaError
    public func createTopics(topicNames: [String], numPartitions: Int32 = 1, replicationFactor: Int32 = -1, timeout: Int32 = 5000) throws -> [Result<KafkaTopicSpecification, KafkaTopicSpecificationError>]  {
        let topics = try topicNames.map { name -> KafkaTopicSpecification in
            try KafkaTopicSpecification(name: name, numPartitions: numPartitions, replicationFactor: replicationFactor)
        }
        return try createTopics(topicSpecifications: topics)
    }

    /// Create topics
    ///
    /// - Parameters:
    ///   - topicSpecifications: Array of topic specification which will be created
    ///   - timeout: Time in ms for how long it should wait for replay from kafka before throwing error. Default is 5000 (5s)
    /// - Returns: Array of result with KafkaTopicSpecification or KafkaTopicSpecificationError
    /// - Throws: KafkaError
    public func createTopics(topicSpecifications: [KafkaTopicSpecification], timeout: Int32 = 5000) throws -> [Result<KafkaTopicSpecification, KafkaTopicSpecificationError>] {
        var topicsPointers = topicSpecifications.map { topic -> OpaquePointer? in topic.pointer }

        let queue = rd_kafka_queue_new(kafkaHandle)
        defer { rd_kafka_queue_destroy(queue) }

        let options = rd_kafka_AdminOptions_new(self.kafkaHandle, RD_KAFKA_ADMIN_OP_CREATETOPICS)
        defer { rd_kafka_AdminOptions_destroy(options) }

        rd_kafka_CreateTopics(kafkaHandle, &topicsPointers, topicsPointers.count, options, queue)
        guard let event = rd_kafka_queue_poll(queue, timeout) else {
            throw KafkaError(description: "Haven't received replay form kafka on time!")
        }
        defer { rd_kafka_event_destroy(event) }

        let errorCode = rd_kafka_event_error(event)
        guard errorCode.rawValue == 0 else {
            throw KafkaError.init(rawValue: Int(errorCode.rawValue))
        }

        let result = rd_kafka_event_CreateTopics_result(event)
        var count: Int = 0
        let cTopicResults = rd_kafka_CreateTopics_result_topics(result, &count)
        let topicResults = topicsToResult(topicResults: cTopicResults, count: count)
        let topicMaps = topicSpecifications.reduce(into: [:]) { dictionary, topicSpecification in dictionary[topicSpecification.name] = topicSpecification }
        
        return topicResults.map { result -> Result<KafkaTopicSpecification, KafkaTopicSpecificationError> in
            switch result {
            case .success(let name):
                return .success(topicMaps[name]!)
            case .failure(let error):
                KafkaClient.logger.warning("Failed to create topic \(error.topicName) with error \(error.kafkaError)")
                return .failure(KafkaTopicSpecificationError(kafkaError: error.kafkaError, topicSpecification: topicMaps[error.topicName]!))
            }
        }
    }
    
    private func topicsToResult(topicResults: UnsafeMutablePointer<OpaquePointer?>?, count: Int) -> [Result<String, ErrorMapping>] {
        var result: [Result<String, ErrorMapping>] = []
        for i in 0..<count {
            let topic = topic_result_by_idx(topicResults, count, i)
            let name = String(cString: rd_kafka_topic_result_name(topic))
            let errorCode = rd_kafka_topic_result_error(topic)
            var res: Result<String, ErrorMapping>
            if errorCode.rawValue == 0 {
                res = .success(name)
            } else {
                res = .failure(ErrorMapping(kafkaError: KafkaError(rawValue: Int(errorCode.rawValue)), topicName: name))
            }
            result.append(res)
        }
        return result
    }
    
}
