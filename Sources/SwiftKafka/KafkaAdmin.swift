//
// Created by Kacper Kawecki on 25/02/2021.
//

import Foundation
import Crdkafka
import Ckafkahelper

public class KafkaAdmin {
    let kafkaHandle: OpaquePointer
    
    private struct ErrorMapping: Error {
        let error: KafkaError
        let topicName: String
    }

    init(client: KafkaClient) {
        self.kafkaHandle = client.kafkaHandle
    }

    //// Create topic if not exists
    /// - Parameter topic: name of the topic to create
    public func createTopics(topicNames: [String], numPartitions: Int32 = 1, replicationFactor: Int32 = -1, timeout: Int32 = 5000) throws -> [KafkaTopicSpecification]  {
        let topics = try topicNames.map { name -> KafkaTopicSpecification in
            try KafkaTopicSpecification(name: name, numPartitions: numPartitions, replicationFactor: replicationFactor)
        }
        return try createTopics(topics: topics)
    }

    public func createTopics(topics: [KafkaTopicSpecification], timeout: Int32 = 5000) throws -> [KafkaTopicSpecification] {
        var topicsPointers = topics.map { topic -> OpaquePointer? in topic.pointer }

        let queue = rd_kafka_queue_new(kafkaHandle)
        defer { rd_kafka_queue_destroy(queue) }

        let options = rd_kafka_AdminOptions_new(self.kafkaHandle, RD_KAFKA_ADMIN_OP_CREATETOPICS)
        defer { rd_kafka_AdminOptions_destroy(options) }

        rd_kafka_CreateTopics(kafkaHandle, &topicsPointers, topicsPointers.count, options, queue)
        guard let event = rd_kafka_queue_poll(queue, timeout) else {
            throw KafkaError(description: "Haven't received replay form kafka")
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
        for res in topicResults {
            switch res {
            case .success(let name):
                    print("Sucessffuly created topic \(name)")
            case .failure(let errMapping):
                print("Failed to create topic \(errMapping.topicName) with error \(errMapping.error)")
            }
        }
        
        return topics
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
                res = .failure(ErrorMapping(error: KafkaError(rawValue: Int(errorCode.rawValue)), topicName: name))
            }
            result.append(res)
        }
        return result
    }
    
}
