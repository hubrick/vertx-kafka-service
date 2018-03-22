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
package com.hubrick.vertx.kafka.consumer.util;

import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import io.prometheus.client.Histogram;

/**
 * @author marcus
 * @since 1.0.0
 */
public class PrometheusMetrics {

    private final static Counter MESSAGES = Counter.build()
            .name("kafka_consumer_verticle_messages_total")
            .help("Total handled messages")
            .labelNames("topic", "consumerGroup", "instanceId")
            .register();

    private final static Gauge IN_PROGRESS_MESSAGES = Gauge.build()
            .name("kafka_consumer_verticle_messages_inprogress")
            .help("In progress messages")
            .labelNames("topic", "consumerGroup", "instanceId")
            .register();

    private final static Histogram DURATION_HISTOGRAM = Histogram.build()
            .name("kafka_consumer_verticle_messages_processing_duration_seconds")
            .help("Message processing duration in seconds")
            .labelNames("topic", "consumerGroup", "instanceId")
            .buckets(0.01D, 0.025D, 0.05D, 0.075D, 0.1D, 0.25D, 0.5D, 0.75D, 1D, 2.5D, 5D, 7.5D, 9.99D)
            .register();

    private final String topic;
    private final String consumerGroup;
    private final String instanceId;

    private PrometheusMetrics(final String topic,
                              final String consumerGroup,
                              final String instanceId) {
        this.topic = topic;
        this.consumerGroup = consumerGroup;
        this.instanceId = instanceId;
    }

    public static PrometheusMetrics create(final String topic, final String consumerGroup, final long instanceId) {
        return new PrometheusMetrics(topic, consumerGroup, String.valueOf(instanceId));
    }

    public InProgressMessage messageStarted() {
        MESSAGES.labels(topic, consumerGroup, instanceId).inc();
        IN_PROGRESS_MESSAGES.labels(topic, consumerGroup, instanceId).inc();
        final Histogram.Timer timer = DURATION_HISTOGRAM.labels(topic, consumerGroup, instanceId).startTimer();
        return () -> {
            timer.observeDuration();
            IN_PROGRESS_MESSAGES.labels(topic, consumerGroup, instanceId).dec();
        };
    }

    @FunctionalInterface
    public interface InProgressMessage {

        void finished();

    }
}
