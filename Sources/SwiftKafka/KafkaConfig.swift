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
import Logging

/**
 The configuration settings for a Kafka consumer/producer.
 These can be set either using the helper functions provided or 
 by subscripting `KafkaConfig` with the configuration key.  
 [Link to configuration keys and descriptions.](https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md)
 ### Usage Example:
 ```swift
 let config = KafkaConfig()
 config.groupId = "Kitura"
 let producer = try? KafkaConsumer(config: config)
 ```
 */
public class KafkaConfig {
    // The pointer to the undelying C `rd_kafka_conf_t` object.
    private var pointer: OpaquePointer
    
    /// :nodoc:
    deinit {
        // deallocate the memory for the `rd_kafka_conf_t`.
        rd_kafka_conf_destroy(pointer)
    }
    /// The internal representation of the Kafka configuration.  
    /// Values can be set by subscripting `KafkaConfig` directly.
    public internal(set) var dictionary = [String: String]()
    
    /// Initialize a KafkaConfig instance with default settings.
    public init() {
        // Create a new `rd_kafka_conf_t` object with default values.
        self.pointer = rd_kafka_conf_new()
        var count = 0
        // Copy the default values into the KafkaConfig dictionary.
        if let dictPointer = rd_kafka_conf_dump(pointer, &count) {
            for i in 0 ... count / 2 {
                if let key = dictPointer.advanced(by: i * 2).pointee,
                   let value = dictPointer.advanced(by: i * 2 + 1).pointee
                {
                    dictionary[String(cString: key)] = String(cString: value)
                }
            }
            // Free the config dump
            rd_kafka_conf_dump_free(dictPointer, count)
        }
    }

    /// Directly set the configuration settings for librdKafka.  
    /// [Link to configuration keys and descriptions.](https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md)
    public subscript(key: String) -> String? {
        get {
            return dictionary[key]
        }
        set {
            if rd_kafka_conf_set(pointer, key, newValue, nil, 0) == RD_KAFKA_CONF_OK {
                dictionary[key] = newValue
            }
        }
    }
    
    // Create a duplicate of the config pointer to be passed to KafkaClient initializers.
    func getPointerDuplicate() -> OpaquePointer {
        return rd_kafka_conf_dup(pointer)
    }
    
    // MARK: Global Configurations
    
    /// Client identifier. Defaults to "rdkafka".
    public var clientID: String {
        set {
            self["client.id"] = newValue
        }
        get {
            return self["client.id"] ?? "rdkafka"
        }
    }
    
    /// Maximum Kafka protocol request message size. Defaults to 1000000.
    public var messageMaxBytes: Int {
        set {
            self["message.max.bytes"] = String(newValue)
        }
        get {
            return Int(self["message.max.bytes"] ?? "1000000") ?? 1000000
        }
    }
    
    /// Maximum size for message to be copied to buffer. Messages larger than this will be passed by reference (zero-copy) at the expense of larger iovecs. Defaults to 65535.
    public var messageCopyMaxBytes: Int {
        set {
            self["message.copy.max.bytes"] = String(newValue)
        }
        get {
            return Int(self["message.copy.max.bytes"] ?? "65535") ?? 65535
        }
    }
    
    /// Maximum Kafka protocol response message size. This serves as a safety precaution to avoid memory exhaustion in case of protocol hickups. This value must be at least fetchMaxBytes + 512 to allow for protocol overhead; the value is adjusted automatically unless the configuration property is explicitly set. Defaults to 100000000.
    public var receiveMessageMaxBytes: Int {
        set {
            self["receive.message.max.bytes"] = String(newValue)
        }
        get {
            return Int(self["receive.message.max.bytes"] ?? "100000000") ?? 100000000
        }
    }
    
    /// Maximum number of in-flight requests per broker connection. This is a generic property applied to all broker communication, however it is primarily relevant to produce requests. In particular, note that other mechanisms limit the number of outstanding consumer fetch request per broker to one. Defaults to 1000000
    public var maxInFlightRequestsPerConnection: Int {
        set {
            self["max.in.flight.requests.per.connection"] = String(newValue)
        }
        get {
            return Int(self["max.in.flight.requests.per.connection"] ?? "1000000") ?? 1000000
        }
    }
    
    /// Non-topic request timeout in milliseconds. This is for metadata requests, etc. Defaults to 60000.
    public var metadataRequestTimeoutMs: Int {
        set {
            self["metadata.request.timeout.ms"] = String(newValue)
        }
        get {
            return Int(self["metadata.request.timeout.ms"] ?? "60000") ?? 60000
        }
    }
    
    /// Topic metadata refresh interval in milliseconds. The metadata is automatically refreshed on error and connect. Use -1 to disable the intervalled refresh. Defaults to 300000
    public var topicMetadataRefreshIntervalMs: Int {
        set {
            self["topic.metadata.refresh.interval.ms"] = String(newValue)
        }
        get {
            return Int(self["topic.metadata.refresh.interval.ms"] ?? "300000") ?? 300000
        }
    }
    
    /// Metadata cache max age. Defaults to topic.metadata.refresh.interval.ms * 3 
    public var metadataMaxAgeMs: Int {
        set {
            self["metadata.max.age.ms"] = String(newValue)
        }
        get {
            return Int(self["metadata.max.age.ms"] ?? "900000") ?? 900000
        }
    }
    
    /// When a topic loses its leader a new metadata request will be enqueued with this initial interval, exponentially increasing until the topic metadata has been refreshed. This is used to recover quickly from transitioning leader brokers. Defaults to 250.
    public var topicMetadataRefreshFastIntervalMs: Int {
        set {
            self["topic.metadata.refresh.fast.interval.ms"] = String(newValue)
        }
        get {
            return Int(self["topic.metadata.refresh.fast.interval.ms"] ?? "250") ?? 250
        }
    }
    
    /// Sparse metadata requests (consumes less network bandwidth). Defaults to true.
    public var topicMetadataRefreshSparse: Bool {
        set {
            self["topic.metadata.refresh.sparse"] = String(newValue)
        }
        get {
            return Bool(self["topic.metadata.refresh.sparse"] ?? "true") ?? true
        }
    }
    
    /// An array of debug contexts to enable. Defaults to an empty array.
    public var debug: [DebugOptions] {
        set {
            let debugStrings = newValue.map{$0.description}
            self["debug"] = debugStrings.joined(separator: ",")
        }
        get {
            let debugCSV = self["debug"] ?? ""
            let debugArray = debugCSV.components(separatedBy: ",")
            let debug = debugArray.map{DebugOptions(description: $0)}
            return debug
        }
    }
    
    /// The possible debug options that can be enabled.
    public struct DebugOptions: CustomStringConvertible {
        
        /// The textual representation of the debug option
        public let description: String
        
        /// Generic debugging.
        public static let generic = DebugOptions(description: "generic")
        /// Broker debugging.
        public static let broker = DebugOptions(description: "broker")
        /// Topic debugging.
        public static let topic = DebugOptions(description: "topic")
        /// Metadata debugging.
        public static let metadata = DebugOptions(description: "metadata")
        /// Feature debugging.
        public static let feature = DebugOptions(description: "feature")
        /// Queue debugging.
        public static let queue = DebugOptions(description: "queue")
        /// Msg debugging.
        public static let msg = DebugOptions(description: "msg")
        /// Protocol debugging.
        public static let `protocol` = DebugOptions(description: "protocol")
        /// Cgrp debugging.
        public static let cgrp = DebugOptions(description: "cgrp")
        /// Security debugging.
        public static let security = DebugOptions(description: "security")
        /// Fetch debugging.
        public static let fetch = DebugOptions(description: "fetch")
        /// Interceptor debugging.
        public static let interceptor = DebugOptions(description: "interceptor")
        /// Plugin debugging.
        public static let plugin = DebugOptions(description: "plugin")
        /// Consumer debugging.
        public static let consumer = DebugOptions(description: "consumer")
        /// Admin debugging.
        public static let admin = DebugOptions(description: "admin")
        /// Eos debugging.
        public static let eos = DebugOptions(description: "eos")
        /// Enable all debugging.
        public static let all = DebugOptions(description: "all")		
    }
    
    /// Default timeout for network requests.  
    /// Producer: ProduceRequests will use the lesser value of socket.timeout.ms and remaining message.timeout.ms for the first message in the batch.  
    /// Consumer: FetchRequests will use fetch.wait.max.ms + socket.timeout.ms. Admin: Admin requests will use socket.timeout.ms  
    /// Defaults to 60000.
    public var socketTimeoutMs: Int {
        set {
            self["socket.timeout.ms"] = String(newValue)
        }
        get {
            return Int(self["socket.timeout.ms"] ?? "60000") ?? 60000
        }
    }
    
    /// Broker socket send buffer size. System default is used if 0. Defaults to 0.
    public var socketSendBufferBytes: Int {
        set {
            self["socket.send.buffer.bytes"] = String(newValue)
        }
        get {
            return Int(self["socket.send.buffer.bytes"] ?? "0") ?? 0
        }
    }
    
    /// Broker socket receive buffer size. System default is used if 0.  Defaults to 0.
    public var socketReceiveBufferBytes: Int {
        set {
            self["socket.receive.buffer.bytes"] = String(newValue)
        }
        get {
            return Int(self["socket.receive.buffer.bytes"] ?? "0") ?? 0
        }
    }
    
    /// Enable TCP keep-alives (SO_KEEPALIVE) on broker sockets. Defaults to false
    public var socketKeepaliveEnable: Bool {
        set {
            self["socket.keepalive.enable"] = String(newValue)
        }
        get {
            return Bool(self["socket.keepalive.enable"] ?? "false") ?? false
        }
    }
    
    /// Disable the Nagle algorithm (TCP_NODELAY) on broker sockets. 
    public var socketNagleDisable: Bool {
        set {
            self["socket.nagle.disable"] = String(newValue)
        }
        get {
            return Bool(self["socket.nagle.disable"] ?? "false") ?? false
        }
    }

    /// Disconnect from broker when this number of send failures (e.g., timed out requests) is reached. Disable with 0. WARNING: It is highly recommended to leave this setting at its default value of 1 to avoid the client and broker to become desynchronized in case of request timeouts. NOTE: The connection is automatically re-established. 
    public var socketMaxFails: Int {
        set {
            self["socket.max.fails"] = String(newValue)
        }
        get {
            return Int(self["socket.max.fails"] ?? "1") ?? 1
        }
    }
    
    /// Allowed broker `IPAddressFamily` (any, IPv4, IPv6). Defaults to "any".
    public var brokerAddressFamily: IPAddressFamily {
        set {
            self["broker.address.family"] = newValue.description
        }
        get {
            return IPAddressFamily(description: self["broker.address.family"] ?? "any")
        }
    }
    
    /// The possible IP address family options that can be set.
    public struct IPAddressFamily: CustomStringConvertible {
        
        /// The textual representation of the IP address family.
        public let description: String
        
        /// Use any IP address family.
        public static let any = IPAddressFamily(description: "any")
        /// Use any IPv4 address family.
        public static let v4 = IPAddressFamily(description: "v4")
        /// Use any IPv6 address family.
        public static let v6 = IPAddressFamily(description: "v6")     
    }
    
    /// The initial time to wait before reconnecting to a broker after the connection has been closed. The time is increased exponentially until reconnect.backoff.max.ms is reached. -25% to +50% jitter is applied to each reconnect backoff. A value of 0 disables the backoff and reconnects immediately. Defaults to 100.
    public var reconnectBackoffMs: Int {
        set {
            self["reconnect.backoff.ms"] = String(newValue)
        }
        get {
            return Int(self["reconnect.backoff.ms"] ?? "100") ?? 100
        }
    }
    
    /// The maximum time to wait before reconnecting to a broker after the connection has been closed. Defaults to 10000.
    public var reconnectBackoffMaxMs: Int {
        set {
            self["reconnect.backoff.max.ms"] = String(newValue)
        }
        get {
            return Int(self["reconnect.backoff.max.ms"] ?? "10000") ?? 10000
        }
    }
    
    /// The `SecurityProtocol` used to communicate with brokers. 
    public var securityProtocol: SecurityProtocol {
        set {
            self["security.protocol"] = newValue.description
        }
        get {
            return SecurityProtocol(description: self["security.protocol"] ?? "plaintext")
        }
    }
    
    /// The possible security protocols that can be used to communicate with brokers.
    public struct SecurityProtocol: CustomStringConvertible {
        
        /// The textual representation of the SecurityProtocol.
        public let description: String
        
        /// Don't use a protocol and send messages in plaintext.
        public static let plaintext = SecurityProtocol(description: "plaintext")
        /// Use the Secure Sockets Layer (SSL) protocol.
        public static let ssl = SecurityProtocol(description: "ssl")
        /// Use the Simple Authentication and Security Layer (SASL) without SSL.
        public static let sasl_plaintext = SecurityProtocol(description: "sasl_plaintext")
        /// Use the Simple Authentication and Security Layer (SASL) with SSL.
        public static let sasl_ssl = SecurityProtocol(description: "sasl_ssl")   
    }
    
    /// Path to client's private key (PEM) used for authentication. Defaults to nil.
    public var sslKeyLocation: String? {
        set {
            self["ssl.key.location"] = newValue
        }
        get {
            return self["ssl.key.location"]
        }
    }
    
    /// Private key passphrase. Defaults to nil.
    public var sslKeyPassword: String? {
        set {
            self["ssl.key.password"] = newValue
        }
        get {
            return self["ssl.key.password"]
        }
    }
    
    /// Path to client's public key (PEM) used for authentication. Defaults to nil.
    public var sslCertificateLocation: String? {
        set {
            self["ssl.certificate.location"] = newValue
        }
        get {
            return self["ssl.certificate.location"]
        }
    }
    
    /// File or directory path to CA certificate(s) for verifying the broker's key. Defaults to nil.
    public var sslCaLocation: String? {
        set {
            self["ssl.ca.location"] = newValue
        }
        get {
            return self["ssl.ca.location"]
        }
    }
    
    /// Path to CRL for verifying broker's certificate validity. Defaults to nil.
    public var sslCrlLocation: String? {
        set {
            self["ssl.crl.location"] = newValue
        }
        get {
            return self["ssl.crl.location"]
        }
    }
    
    /// Path to client's keystore (PKCS#12) used for authentication. Defaults to nil.
    public var sslKeystoreLocation: String? {
        set {
            self["ssl.keystore.location"] = newValue
        }
        get {
            return self["ssl.keystore.location"]
        }
    }
    
    /// Client's keystore (PKCS#12) password. Defaults to nil.
    public var sslKeystorePassword: String? {
        set {
            self["ssl.keystore.password"] = newValue
        }
        get {
            return self["ssl.keystore.password"]
        }
    }
    
    /// SASL mechanism to use for authentication. Defaults to GSSAPI.
    public var saslMechanism: SASLMechanism {
        set {
            self["sasl.mechanisms"] = newValue.description
        }
        get {
            return SASLMechanism(description: self["sasl.mechanisms"] ?? "GSSAPI")
        }
    }
    
    /// The possible SASL mechanisms that can be used for authentication.
    public struct SASLMechanism: CustomStringConvertible {
        
        /// The textual representation of the SASLMechanism.
        public let description: String
        
        /// Use the GSSAPI mechanism.
        public static let gssapi = SASLMechanism(description: "GSSAPI")
        /// Use the PLAIN mechanism.
        public static let plain = SASLMechanism(description: "PLAIN")
        /// Use the SCRAM-SHA-256 mechanism.
        public static let scramSHA256 = SASLMechanism(description: "SCRAM-SHA-256")
        /// Use the SCRAM-SHA-512 mechanism.
        public static let scramSHA512 = SASLMechanism(description: "SCRAM-SHA-512")
        /// Use the oauthbearer mechanism.
        public static let oauthbearer = SASLMechanism(description: "OAUTHBEARER")   
    }
    
    /// SASL username for use with the PLAIN, SCRAM-SHA-256 and SCRAM-SHA-512 mechanisms. Defaults to nil.
    public var saslUsername: String? {
        set {
            self["sasl.username"] = newValue
        }
        get {
            return self["sasl.username"]
        }
    }
    
    /// SASL password for use with the PLAIN, SCRAM-SHA-256 and SCRAM-SHA-512 mechanisms. Defaults to nil.
    public var saslPassword: String? {
        set {
            self["sasl.password"] = newValue
        }
        get {
            return self["sasl.password"]
        }
    }
    
    // MARK: Consumer Configurations

    /// Consumer Only. Client group id string. All clients sharing the same group.id belong to the same group and the messages will be split between them. Defaults to nil, meaning a random UUID String will be used as the groupId.
    public var groupId: String? {
        set {
            self["group.id"] = newValue
        }
        get {
            return self["group.id"]
        }
    }
    
    /// Consumer Only. Client group session and failure detection timeout. The consumer sends periodic heartbeats (heartbeat.interval.ms) to indicate its liveness to the broker. If no hearts are received by the broker for a group member within the session timeout, the broker will remove the consumer from the group and trigger a rebalance. Defaults to 10000.
    public var sessionTimeoutMs: Int {
        set {
            self["session.timeout.ms"] = String(newValue)
        }
        get {
            return Int(self["session.timeout.ms"] ?? "10000") ?? 10000
        }
    }
    
    /// Consumer Only. Group session keepalive heartbeat interval. Defaults to 3000.
    public var heartbeatIntervalMs: Int {
        set {
            self["heartbeat.interval.ms"] = String(newValue)
        }
        get {
            return Int(self["heartbeat.interval.ms"] ?? "3000") ?? 3000
        }
    }
    
    /// Consumer Only. Maximum allowed time between calls to consume messages (e.g., rd_kafka_consumer_poll()) for high-level consumers. If this interval is exceeded the consumer is considered failed and the group will rebalance in order to reassign the partitions to another consumer group member. Warning: Offset commits may be not possible at this point. Note: It is recommended to set enable.auto.offset.store=false for long-time processing applications and then explicitly store offsets (using offsets_store()) after message processing, to make sure offsets are not auto-committed prior to processing has finished. The interval is checked two times per second. Defaults to 300000.
    public var maxPollIntervalMs: Int {
        set {
            self["max.poll.interval.ms"] = String(newValue)
        }
        get {
            return Int(self["max.poll.interval.ms"] ?? "300000") ?? 300000
        }
    }
    
    /// Consumer Only. Automatically and periodically commit offsets in the background. Note: setting this to false does not prevent the consumer from fetching previously committed start offsets. To circumvent this behaviour set specific start offsets per partition in the call to assign(). Defaults to true.
    public var enableAutoCommit: Bool {
        set {
            self["enable.auto.commit"] = String(newValue)
        }
        get {
            return Bool(self["enable.auto.commit"] ?? "true") ?? true
        }
    }
    
    /// Consumer Only. The frequency in milliseconds that the consumer offsets are committed (written) to offset storage. (0 = disable). Default to 5000.
    public var autoCommitIntervalMs: Int {
        set {
            self["auto.commit.interval.ms"] = String(newValue)
        }
        get {
            return Int(self["auto.commit.interval.ms"] ?? "5000") ?? 5000
        }
    }
    
    /// Consumer Only. Automatically store offset of last message provided to application. The offset store is an in-memory store of the next offset to (auto-)commit for each partition. Defaults to true
    public var enableAutoOffsetStore: Bool {
        set {
            self["enable.auto.offset.store"] = String(newValue)
        }
        get {
            return Bool(self["enable.auto.offset.store"] ?? "true") ?? true
        }
    }
    
    /// Consumer Only. Emit RD_KAFKA_RESP_ERR__PARTITION_EOF event whenever the consumer reaches the end of a partition. defaults to true.
    public var enablePartitionEOF: Bool {
        set {
            self["enable.partition.eof"] = String(newValue)
        }
        get {
            return Bool(self["enable.partition.eof"] ?? "true") ?? true
        }
    }
    
    // MARK: Producer Configurations

    /// Producer only. When set to true, the producer will ensure that messages are successfully produced exactly once and in the original produce order. The following configuration properties are adjusted automatically (if not modified by the user) when idempotence is enabled: max.in.flight.requests.per.connection=5 (must be less than or equal to 5), retries=INT32_MAX (must be greater than 0), acks=all, queuing.strategy=fifo. Producer instantation will fail if user-supplied configuration is incompatible. Defaults to false.
    public var enableIdempotence: Bool {
        set {
            self["enable.idempotence"] = String(newValue)
        }
        get {
            return Bool(self["enable.idempotence"] ?? "false") ?? false
        }
    }
    
    /// Producer only. Maximum number of messages allowed on the producer queue. This queue is shared by all topics and partitions. Defaults to 100000.
    public var queueBufferingMaxMessages: Int {
        set {
            self["queue.buffering.max.messages"] = String(newValue)
        }
        get {
            return Int(self["queue.buffering.max.messages"] ?? "100000") ?? 100000
        }
    }
    
    /// Producer only. Maximum total message size sum allowed on the producer queue. This queue is shared by all topics and partitions. This property has higher priority than queue.buffering.max.messages. Defaults to 1048576.
    public var queueBufferingMaxKBytes: Int {
        set {
            self["queue.buffering.max.kbytes"] = String(newValue)
        }
        get {
            return Int(self["queue.buffering.max.kbytes"] ?? "1048576") ?? 1048576
        }
    }
    
    /// Producer only. Delay in milliseconds to wait for messages in the producer queue to accumulate before constructing message batches (MessageSets) to transmit to brokers. A higher value allows larger and more effective (less overhead, improved compression) batches of messages to accumulate at the expense of increased message delivery latency. Defaults to 0.
    public var queueBufferingMaxMs: Int {
        set {
            self["queue.buffering.max.ms"] = String(newValue)
        }
        get {
            return Int(self["queue.buffering.max.ms"] ?? "0") ?? 0
        }
    }
    
    /// Producer only. How many times to retry sending a failing Message. Note: retrying may cause reordering unless enable.idempotence is set to true. Defaults to 2.
    public var messageSendMaxRetries: Int {
        set {
            self["message.send.max.retries"] = String(newValue)
        }
        get {
            return Int(self["message.send.max.retries"] ?? "2") ?? 2
        }
    }
    
    // MARK: Topic Configurations

    /// Producer only. This field indicates the number of acknowledgements the leader broker must receive from ISR brokers before responding to the request: 0=Broker does not send any response/ack to client, -1=Broker will block until message is committed by all in sync replicas (ISRs). Defaults to -1.
    public var requestRequiredAcks: Int {
        set {
            self["request.required.acks"] = String(newValue)
        }
        get {
            return Int(self["request.required.acks"] ?? "-1") ?? -1
        }
    }
    
    /// Producer only. The ack timeout of the producer request in milliseconds. This value is only enforced by the broker and relies on request.required.acks being != 0. Default 5000.
    public var requestTimeoutMs: Int {
        set {
            self["request.timeout.ms"] = String(newValue)
        }
        get {
            return Int(self["request.timeout.ms"] ?? "5000") ?? 5000
        }
    }
    
    /// Producer only. Local message timeout. This value is only enforced locally and limits the time a produced message waits for successful delivery. A time of 0 is infinite. This is the maximum time Kitura-Kafka may use to deliver a message (including retries). Delivery error occurs when either the retry count or the message timeout are exceeded. Defaults to 300000.
    public var messageTimeoutMs: Int {
        set {
            self["message.timeout.ms"] = String(newValue)
        }
        get {
            return Int(self["message.timeout.ms"] ?? "300000") ?? 300000
        }
    }
    
    
    /// Consumer only. Action to take when there is no initial offset in offset store or the desired offset is out of range: 'smallest','earliest', 'beginning' - automatically reset the offset to the smallest offset, 'largest','latest', 'end' - automatically reset the offset to the largest offset, 'error' - trigger an error which is retrieved by consuming message. Defaults to largest.
    public var autoOffsetReset: AutoResetOptions {
        set {
            self["auto.offset.reset"] = newValue.description
        }
        get {
            return AutoResetOptions(description: self["auto.offset.reset"] ?? "largest")
        }
    }
    

    /// A struct representing the options actions to take when there is no initial offset in offset store or the desired offset is out of range.
    public struct AutoResetOptions: CustomStringConvertible {
        
        /// The textual representation of the AutoResetOptions.
        public let description: String
        
        /// Automatically reset the offset to the smallest offset.
        public static let smallest = AutoResetOptions(description: "smallest")
        /// Automatically reset the offset to the smallest offset.
        public static let earliest = AutoResetOptions(description: "earliest")
        /// Automatically reset the offset to the smallest offset.
        public static let beginning = AutoResetOptions(description: "beginning")
        /// Automatically reset the offset to the largest offset
        public static let largest = AutoResetOptions(description: "largest")
        /// Automatically reset the offset to the largest offset
        public static let latest = AutoResetOptions(description: "latest")
        /// Automatically reset the offset to the largest offset
        public static let end = AutoResetOptions(description: "end")
        /// Trigger an error which is retrieved by consuming message.
        public static let error = AutoResetOptions(description: "error")

    }
    
    /// Convert C closure to Swift closure provided by the user when they initialize a KafkaProducer.
    func setDeliveredMessageCallback() {
        // Set the config "dr_msg_cb" for this KafkaConfig pointer.
        // This is a C closure so cannot capture context.
        // Instead you are returned a kafkaHandle to identify the producer this was sent by.
        // The third input should be a id pointer provided by the producer send however this didn't seem to work.
        rd_kafka_conf_set_dr_msg_cb(self.pointer, { (kafkaHandle, message, _) -> Void in
            guard let kafkaHandle = kafkaHandle, let message = message else {
                KafkaProducer.logger.error("Internal error: No message returned in message callback.")
                return
            }
            KafkaProducer.callbackSemaphore.wait()
            guard let userCallback = KafkaProducer.kafkaHandleToMessageCallback[kafkaHandle]?[message.pointee._private] else {
                KafkaProducer.callbackSemaphore.signal()
                // No callback set or the KafkaClient is a consumer
                return
            }
            // Callback will only be called once so remove idPointer and deallocate it
            KafkaProducer.kafkaHandleToMessageCallback[kafkaHandle]?[message.pointee._private] = nil
            message.pointee._private.deallocate()
            KafkaProducer.callbackSemaphore.signal()

            // Check if returned message is an error
            if message.pointee.err.rawValue != 0 {
                let error = KafkaError(rawValue: Int(message.pointee.err.rawValue))
                return userCallback(.failure(error))
            }
            // Try to read the returned massage as a KafkaConsumerRecord.
            guard let consumerRecord = KafkaConsumerRecord(messagePointer: UnsafeMutablePointer(mutating: message)) else {
                let error = KafkaError(description: "Produce message failed with unknown error")
                return userCallback(.failure(error))
            }
            return userCallback(.success(consumerRecord))
        })
    }
}
