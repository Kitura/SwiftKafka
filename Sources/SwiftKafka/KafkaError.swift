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

/// Representation of errors that can be thrown by KituraKafka.
/// This maps the underlying [rdKafka errors](https://github.com/edenhill/librdkafka/blob/master/src/rdkafka.h#L253)
public struct KafkaError: RawRepresentable, Equatable, Hashable, Error, CustomStringConvertible {
    /// Create a KafkaError from an rdKafka error code.
    public init(rawValue: Int) {
        self.rawValue = rawValue
        self.description = KafkaError.reason(forKafkaCode: rawValue)
    }
    
    init(description: String) {
        self.rawValue = -1
        self.description = description
    }
    
    /// The error code.
    public var rawValue: Int
    
    /// Required for RawRepresentable.
    public typealias RawValue = Int
    
    /// The human readable error description.
    public let description: String
        
}
extension KafkaError {
    
    private static func reason(forKafkaCode code: Int) -> String {
        switch code {
        /* Internal errors to rdkafka: */
        case -199: return "Received message is incorrect"
        case -198: return "Bad/unknown compression"
        case -197: return "Broker is going away"
        case -196: return "Generic failure"
        case -195: return "Broker transport failure"
        case -194: return "Critical system resource"
        case -193: return "Failed to resolve broker"
        case -192: return "Produced message timed out"
        case -191: return "Reached the end of the topic+partition queue on the broker"
        case -190: return "Permanent: Partition does not exist in cluster."
        case -189: return "File or filesystem error"
        case -188: return "Permanent: Topic does not exist in cluster."
        case -187: return "All broker connections are down."
        case -186: return "Invalid argument, or invalid configuration"
        case -185: return "Operation timed out"
        case -184: return "Queue is full"
        case -183: return "ISR count < required.acks"
        case -182: return "Broker node update"
        case -181: return "SSL error"
        case -180: return "Waiting for coordinator to become available."
        case -179: return "Unknown client group"
        case -178: return "Operation in progress"
        case -177: return "Previous operation in progress, wait for it to finish."
        case -176: return "This operation would interfere with an existing subscription"
        case -175: return "Assigned partitions (rebalance_cb)"
        case -174: return "Revoked partitions (rebalance_cb)"
        case -173: return "Conflicting use"
        case -172: return "Wrong state"
        case -171: return "Unknown protocol"
        case -170: return "Not implemented"
        case -169: return "Authentication failure"
        case -168: return "No stored offset"
        case -167: return "Outdated"
        case -166: return "Timed out in queue"
        case -165: return "Feature not supported by broker"
        case -164: return "Awaiting cache update"
        case -163: return "Operation interrupted (e.g., due to yield))"
        case -162: return "Key serialization error"
        case -161: return "Value serialization error"
        case -160: return "Key deserialization error"
        case -159: return "Value deserialization error"
        case -158: return "Partial response"
        case -157: return "Modification attempted on read-only object"
        case -156: return "No such entry / item not found"
        case -155: return "Read underflow"
        case -154: return "Invalid type"
        case -153: return "Retry operation"
        case -152: return "Purged in queue"
        case -151: return "Purged in flight"
        case -150: return "Fatal error: see rd_kafka_fatal_error()"
        case -149: return "Inconsistent state"
        case -148: return "Gap-less ordering would not be guaranteed if proceeding"
        case -147: return "Maximum poll interval exceeded"
        /* Kafka broker errors: */
        case 1: return "Offset out of range"
        case 2: return "Invalid message"
        case 3: return "Unknown topic or partition"
        case 4: return "Invalid message size"
        case 5: return "Leader not available"
        case 6: return "Not leader for partition"
        case 7: return "Request timed out"
        case 8: return "Broker not available"
        case 9: return "Replica not available"
        case 10: return "Message size too large"
        case 11: return "StaleControllerEpochCode"
        case 12: return "Offset metadata string too large"
        case 13: return "Broker disconnected before response received"
        case 14: return "Group coordinator load in progress"
        case 15: return "Group coordinator not available"
        case 16: return "Not coordinator for group"
        case 17: return "Invalid topic"
        case 18: return "Message batch larger than configured server segment size"
        case 19: return "Not enough in-sync replicas"
        case 20: return "Message(s) written to insufficient number of in-sync replicas"
        case 21: return "Invalid required acks value"
        case 22: return "Specified group generation id is not valid"
        case 23: return "Inconsistent group protocol"
        case 24: return "Invalid group.id"
        case 25: return "Unknown member"
        case 26: return "Invalid session timeout"
        case 27: return "Group rebalance in progress"
        case 28: return "Commit offset data size is not valid"
        case 29: return "Topic authorization failed"
        case 30: return "Group authorization failed"
        case 31: return "Cluster authorization failed"
        case 32: return "Invalid timestamp"
        case 33: return "Unsupported SASL mechanism"
        case 34: return "Illegal SASL state"
        case 35: return "Unuspported version"
        case 36: return "Topic already exists"
        case 37: return "Invalid number of partitions"
        case 38: return "Invalid replication factor"
        case 39: return "Invalid replica assignment"
        case 40: return "Invalid config"
        case 41: return "Not controller for cluster"
        case 42: return "Invalid request"
        case 43: return "Message format on broker does not support request"
        case 44: return "Policy violation"
        case 45: return "Broker received an out of order sequence number"
        case 46: return "Broker received a duplicate sequence number"
        case 47: return "Producer attempted an operation with an old epoch"
        case 48: return "Producer attempted a transactional operation in an invalid state"
        case 49: return "Producer attempted to use a producer id which is not currently assigned to its transactional id"
        case 50: return "Transaction timeout is larger than the maximum value allowed by the broker's max.transaction.timeout.ms"
        case 51: return "Producer attempted to update a transaction while another concurrent operation on the same transaction was ongoing"
        case 52: return "Indicates that the transaction coordinator is no longer the current coordinator for a given producer"
        case 53: return "Transactional Id authorization failed"
        case 54: return "Security features are disabled"
        case 55: return "Operation not attempted"
        case 56: return "Disk error when trying to access log file on the disk"
        case 57: return "The user-specified log directory is not found in the broker config"
        case 58: return "SASL Authentication failed"
        case 59: return "Unknown Producer Id"
        case 60: return "Partition reassignment is in progress"
        case 61: return "Delegation Token feature is not enabled"
        case 62: return "Delegation Token is not found on server"
        case 63: return "Specified Principal is not valid Owner/Renewer"
        case 64: return "Delegation Token requests are not allowed on this connection"
        case 65: return "Delegation Token authorization failed"
        case 66: return "Delegation Token is expired"
        case 67: return "Supplied principalType is not supported"
        case 68: return "The group is not empty"
        case 69: return "The group id does not exist"
        case 70: return "The fetch session ID was not found"
        case 71: return "The fetch session epoch is invalid"
        case 72: return "No matching listener"
        case 73: return "Topic deletion is disabled"
        case 74: return "Unsupported compression type"
        default: return "Unknown error"
        }
    }

}
