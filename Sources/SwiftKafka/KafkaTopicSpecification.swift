//
// Created by kacper on 24/02/2021.
//

import Foundation
import Crdkafka
import Logging


public class KafkaTopicSpecification {
    // The pointer to the underlying C `rd_kafka_NewTopic_t` object.
    var pointer: OpaquePointer?
    public let name: String
    public let numPartitions: Int32
    public let replicationFactor: Int32
    internal var _options: [String: String]
    public var options: [String: String] { return self._options }

    deinit {
        if let pointer = pointer {
            rd_kafka_NewTopic_destroy(pointer)
        }
    }

    public init(name: String, numPartitions: Int32 = 1, replicationFactor: Int32 = -1) throws {
        self.name = name
        self.numPartitions = numPartitions
        self.replicationFactor = replicationFactor
        let errorCString = UnsafeMutablePointer<CChar>.allocate(capacity: KafkaConsumer.stringSize)
        defer { errorCString.deallocate() }
        guard let pointer = rd_kafka_NewTopic_new(name, numPartitions, replicationFactor, errorCString, KafkaConsumer.stringSize) else {
            throw KafkaError(description: String(cString: errorCString))
        }
        self.pointer = pointer
        self._options = [:]
    }

    public func setOption(key: String, value: String) throws -> [String: String] {
        let errorCode = rd_kafka_NewTopic_set_config(pointer, key, value)
        guard errorCode.rawValue == 0 else {
            throw KafkaError(rawValue: Int(errorCode.rawValue))
        }
        _options[key] = value
        return _options
    }
}
