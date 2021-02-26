//
// Created by Kacper Kawecki on 25/02/2021.
//

import Foundation
import Crdkafka
import Ckafkahelper

public class KafkaAdmin {
    let kafkaHandle: OpaquePointer
    
    public struct KafkaTopicError: Error {
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

    /// Create topics in cluster
    ///
    /// The list of TopicSpecification objects define the per-topic partition count, replicas, etc.
    ///
    /// Topic creation is non-atomic and may succeed for some topics but fail for others,
    /// make sure to check the result for topic-specific errors.
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

    private class KafkaDeleteTopic {
        let name: String
        let pointer: OpaquePointer?

        init(_ name: String) throws {
            self.name = name
            guard let pointer = rd_kafka_DeleteTopic_new(name.cString(using: .utf8)) else {
                throw KafkaError(description: "Invalid arguments for topic \(name)")
            }
            self.pointer = pointer
        }

        deinit {
            if pointer != nil {
                rd_kafka_DeleteTopic_destroy(pointer)
            }
        }
    }

    /// DeleteTopics deletes a batch of topics.
    ///
    /// This operation is not transactional and may succeed for a subset of topics while failing others.
    /// It may take several seconds after the DeleteTopics result returns success for
    /// all the brokers to become aware that the topics are gone. During this time,
    /// topic metadata and configuration may continue to return information about deleted topics.
    //    ///
    //    /// Requires broker version >= 0.10.1.0
    ///
    /// - Parameters:
    ///   - topicsNames: Topic names to be deleted
    ///   - timeout: Time in ms for how long it should wait for replay from kafka before throwing error. Default is 5000 (5s)
    /// - Returns: Array of result with topic name or KafkaTopicError
    /// - Throws: KafkaError
    public func deleteTopics(topicsNames: [String], timeout: Int32 = 5000) throws -> [Result<String, KafkaTopicError>] {
        let topics = try topicsNames.map { name in try KafkaDeleteTopic(name) }

        return try deleteTopics(topics: topics, timeout: timeout)
    }

    private func deleteTopics(topics: [KafkaDeleteTopic], timeout: Int32) throws -> [Result<String, KafkaTopicError>] {
        let queue = rd_kafka_queue_new(kafkaHandle)
        defer { rd_kafka_queue_destroy(queue) }

        let options = rd_kafka_AdminOptions_new(kafkaHandle, RD_KAFKA_ADMIN_OP_DELETETOPICS)
        defer { rd_kafka_AdminOptions_destroy(options) }

        var pointers = topics.map { topic in topic.pointer }

        rd_kafka_DeleteTopics(kafkaHandle, &pointers, pointers.count, options, queue)

        guard let event = rd_kafka_queue_poll(queue, timeout) else {
            throw KafkaError(description: "Haven't received replay form kafka on time!")
        }
        defer { rd_kafka_event_destroy(event) }

        let errorCode = rd_kafka_event_error(event)
        guard errorCode.rawValue == 0 else {
            throw KafkaError.init(rawValue: Int(errorCode.rawValue))
        }
        KafkaClient.logger.debug("Parsing response for deleting \(topics)") // Ensure that topic objects are not destroyed before that

        let result = rd_kafka_event_DeleteTopics_result(event)
        var count: Int = 0
        let cTopicResults = rd_kafka_DeleteTopics_result_topics(result, &count)
        return topicsToResult(topicResults: cTopicResults, count: count)
    }
    
    private func topicsToResult(topicResults: UnsafeMutablePointer<OpaquePointer?>?, count: Int) -> [Result<String, KafkaTopicError>] {
        var result: [Result<String, KafkaTopicError>] = []
        for i in 0..<count {
            let topic = topic_result_by_idx(topicResults, count, i)
            let name = String(cString: rd_kafka_topic_result_name(topic))
            let errorCode = rd_kafka_topic_result_error(topic)
            var res: Result<String, KafkaTopicError>
            if errorCode.rawValue == 0 {
                res = .success(name)
            } else {
                res = .failure(KafkaTopicError(kafkaError: KafkaError(rawValue: Int(errorCode.rawValue)), topicName: name))
            }
            result.append(res)
        }
        return result
    }
    
}
