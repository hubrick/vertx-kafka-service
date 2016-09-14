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
package com.hubrick.vertx.kafka.producer.config;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * @author Emir Dizdarevic
 * @since 1.0.0
 */
public class KafkaProducerConfiguration {

    private final String defaultTopic;
    private final String brokerList;
    private final Integer requestAcks;

    private ProducerType type = ProducerType.SYNC;
    private Integer maxRetries = 3;
    private Integer retryBackoffMs = 100;
    private Integer bufferingMaxMs = 5000;
    private Integer bufferingMaxMessages = 10000;
    private Integer enqueueTimeout = -1;
    private Integer batchMessageNum = 200;
    private StatsDConfiguration statsDConfiguration;

    public KafkaProducerConfiguration(String defaultTopic,
                                      String brokerList,
                                      Integer requestAcks) {
        checkNotNull(defaultTopic, "defaultTopic must not be null");
        checkNotNull(brokerList, "brokerList must not be null");
        checkNotNull(requestAcks, "requestAcks must not be null");

        this.defaultTopic = defaultTopic;
        this.brokerList = brokerList;
        this.requestAcks = requestAcks;
    }

    public String getDefaultTopic() {
        return defaultTopic;
    }

    public String getBrokerList() {
        return brokerList;
    }

    public Integer getRequestAcks() {
        return requestAcks;
    }

    public ProducerType getType() {
        return type;
    }

    public void setType(ProducerType type) {
        this.type = type;
    }

    public Integer getMaxRetries() {
        return maxRetries;
    }

    public void setMaxRetries(Integer maxRetries) {
        checkArgument(maxRetries >= 0, "maxRetries must be greater or equal to 0");
        this.maxRetries = maxRetries;
    }

    public Integer getRetryBackoffMs() {
        return retryBackoffMs;
    }

    public void setRetryBackoffMs(Integer retryBackoffMs) {
        checkArgument(retryBackoffMs > 0, "retryBackoffMs must be greater then 0");

        this.retryBackoffMs = retryBackoffMs;
    }

    public Integer getBufferingMaxMs() {
        return bufferingMaxMs;
    }

    public void setBufferingMaxMs(Integer bufferingMaxMs) {
        checkArgument(bufferingMaxMs > 0, "bufferingMaxMs must be greater then 0");

        this.bufferingMaxMs = bufferingMaxMs;
    }

    public Integer getBufferingMaxMessages() {
        return bufferingMaxMessages;
    }

    public void setBufferingMaxMessages(Integer bufferingMaxMessages) {
        checkArgument(bufferingMaxMessages > 0, "bufferingMaxMessages must be greater then 0");

        this.bufferingMaxMessages = bufferingMaxMessages;
    }

    public Integer getEnqueueTimeout() {
        return enqueueTimeout;
    }

    public void setEnqueueTimeout(Integer enqueueTimeout) {
        checkArgument(enqueueTimeout == -1 || enqueueTimeout == 0, "enqueueTimeout can be either -1 or 0");

        this.enqueueTimeout = enqueueTimeout;
    }

    public Integer getBatchMessageNum() {
        return batchMessageNum;
    }

    public void setBatchMessageNum(Integer batchMessageNum) {
        checkArgument(batchMessageNum > 0, "batchMessageNum must be greater then 0");

        this.batchMessageNum = batchMessageNum;
    }

    public StatsDConfiguration getStatsDConfiguration() {
        return statsDConfiguration;
    }

    public void setStatsDConfiguration(StatsDConfiguration statsDConfiguration) {
        this.statsDConfiguration = statsDConfiguration;
    }
}
