/**
 * Copyright (C) 2016 Etaia AS (oss@hubrick.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hubrick.vertx.kafka.consumer.config;

/**
 * Configuration Options for the Kafka Consumer.
 *
 * @author Marcus Thiesen
 * @since 1.0.0
 */
public class KafkaConsumerConfiguration {

    private final String clientId;
    private final String groupId;
    private final String kafkaTopic;
    private final String bootstrapServers;
    private final String offsetReset;
    private final int maxUnacknowledged;
    private final long maxUncommitedOffsets;
    private final long ackTimeoutSeconds;
    private final long commitTimeoutMs;
    private final int maxRetries;
    private final int initialRetryDelaySeconds;
    private final int maxRetryDelaySeconds;
    private final long eventBusSendTimeout;
    private final double messagesPerSecond;
    private final String keyDeserializer;
    private final String valueDeserializer;

    private KafkaConsumerConfiguration(final String groupId,
                                       final String clientId,
                                       final String kafkaTopic,
                                       final String bootstrapServers,
                                       final String offsetReset,
                                       final int maxUnacknowledged,
                                       final long maxUncommittedOffset,
                                       final long ackTimeoutSeconds,
                                       final long commitTimeoutMs,
                                       final int maxRetries,
                                       final int initialRetryDelaySeconds,
                                       final int maxRetryDelaySeconds,
                                       final long eventBusSendTimeout,
                                       final double messagesPerSecond,
                                       final String keyDeserializer,
                                       final String valueDeserializer) {
        this.clientId = clientId;
        this.groupId = groupId;
        this.kafkaTopic = kafkaTopic;
        this.bootstrapServers = bootstrapServers;
        this.offsetReset = offsetReset;
        this.maxUnacknowledged = maxUnacknowledged;
        this.maxUncommitedOffsets = maxUncommittedOffset;
        this.ackTimeoutSeconds = ackTimeoutSeconds;
        this.commitTimeoutMs = commitTimeoutMs;
        this.maxRetries = maxRetries;
        this.initialRetryDelaySeconds = initialRetryDelaySeconds;
        this.maxRetryDelaySeconds = maxRetryDelaySeconds;
        this.eventBusSendTimeout = eventBusSendTimeout;
        this.messagesPerSecond = messagesPerSecond;
        this.keyDeserializer = keyDeserializer;
        this.valueDeserializer = valueDeserializer;
    }

    public static KafkaConsumerConfiguration create(final String groupId,
                                                    final String clilentId,
                                                    final String kafkaTopic,
                                                    final String bootstrapServers,
                                                    final String offsetReset,
                                                    final int maxUnacknowledged,
                                                    final long maxUncommittedOffsets,
                                                    final long ackTimeoutSeconds,
                                                    final long commitTimeoutMs,
                                                    final int maxRetries,
                                                    final int initialRetryDelaySeconds,
                                                    final int maxRetryDelaySeconds,
                                                    final long eventBusSendTimeout,
                                                    final double messagesPerSecond,
                                                    final String keyDeserializer,
                                                    final String valueDeserializer) {
        return new KafkaConsumerConfiguration(
                groupId,
                clilentId,
                kafkaTopic,
                bootstrapServers,
                offsetReset,
                maxUnacknowledged,
                maxUncommittedOffsets,
                ackTimeoutSeconds,
                commitTimeoutMs,
                maxRetries,
                initialRetryDelaySeconds,
                maxRetryDelaySeconds,
                eventBusSendTimeout,
                messagesPerSecond,
                keyDeserializer,
                valueDeserializer);
    }

    public String getGroupId() {
        return groupId;
    }

    public String getKafkaTopic() {
        return kafkaTopic;
    }

    public String getOffsetReset() {
        return offsetReset;
    }

    public int getMaxUnacknowledged() {
        return maxUnacknowledged;
    }

    public long getMaxUncommitedOffsets() {
        return maxUncommitedOffsets;
    }

    public long getAckTimeoutSeconds() {
        return ackTimeoutSeconds;
    }

    public long getCommitTimeoutMs() {
        return commitTimeoutMs;
    }

    public int getMaxRetryDelaySeconds() {
        return maxRetryDelaySeconds;
    }

    public int getInitialRetryDelaySeconds() {
        return initialRetryDelaySeconds;
    }

    public int getMaxRetries() {
        return maxRetries;
    }

    public long getEventBusSendTimeout() {
        return eventBusSendTimeout;
    }

    public String getClientId() {
        return clientId;
    }

    public String getBootstrapServers() {
        return bootstrapServers;
    }

    public double getMessagesPerSecond() {
        return messagesPerSecond;
    }

    public String getKeyDeserializer() {
        return keyDeserializer;
    }

    public String getValueDeserializer() {
        return valueDeserializer;
    }
}
