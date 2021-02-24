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
        let errstr = UnsafeMutablePointer<CChar>.allocate(capacity: KafkaConsumer.stringSize)
        defer { errstr.deallocate() }
        guard let pointer = rd_kafka_NewTopic_new(name, numPartitions, replicationFactor, errstr, KafkaConsumer.stringSize) else {
            throw KafkaError(description: String(cString: errstr))
        }
        self.pointer = pointer
    }
}
