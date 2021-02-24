//
// Created by kacper on 24/02/2021.
//

import Foundation
import Crdkafka
import Logging


internal class KafkaTopic {
    // The pointer to the undelying C `rd_kafka_conf_t` object.
    var pointer: OpaquePointer?
    let name: String
    let numPartitions: Int32
    let replicationFactor: Int32

    deinit {
        if let pointer = pointer {
            rd_kafka_NewTopic_destroy(pointer)
        }
    }

    init(name: String, numPartitions: Int32 = 1, replicationFactor: Int32 = -1) throws {
        self.name = name
        self.numPartitions = numPartitions
        self.replicationFactor = replicationFactor
        var error: Data = Data(capacity: 512)
        var errorMessage: String?
        error.withUnsafeMutableBytes({ (bytes: UnsafeMutablePointer<Int8>) -> Void in
            self.pointer = rd_kafka_NewTopic_new(name, numPartitions, replicationFactor, bytes, 512)
            if self.pointer == nil {
                errorMessage = String(cString: bytes)
            }
        })
        if let errorMessage = errorMessage {
            throw KafkaError(description: errorMessage)
        }
    }
}
