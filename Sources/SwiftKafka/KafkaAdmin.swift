//
// Created by Kacper Kawecki on 25/02/2021.
//

import Foundation
import Crdkafka

public class KafkaAdmin {
    let kafkaHandle: OpaquePointer

    init(client: KafkaClient) {
        self.kafkaHandle = client.kafkaHandle
    }

    //// Create topic if not exists
    /// - Parameter topic: name of the topic to create
    public func createTopics(topicNames: [String], numPartitions: Int32 = 1, replicationFactor: Int32 = -1, timeout: Int32 = 5000) throws -> [KafkaNewTopic]  {
        let topics = try topicNames.map { name -> KafkaNewTopic in
            try KafkaNewTopic(name: name, numPartitions: numPartitions, replicationFactor: replicationFactor)
        }
        return try createTopics(topics: topics)
    }

    public func createTopics(topics: [KafkaNewTopic], timeout: Int32 = 5000) throws -> [KafkaNewTopic] {
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
        var count: Int32
        let topicResults = rd_kafka_CreateTopics_result_topics(result, &count)

        return topics
    }
}
