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
package com.hubrick.vertx.kafka.consumer;

import com.hubrick.vertx.kafka.consumer.config.KafkaConsumerConfiguration;
import com.hubrick.vertx.kafka.consumer.property.KafkaConsumerProperties;
import io.vertx.core.json.JsonObject;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/**
 * @author marcus
 * @since 1.0.0
 */
public class KafkaConsumerConfigurationTest {

    public static final String CLIENT_ID = "clientId";
    public static final String TOPIC = "topic";
    public static final String GROUP_ID = "groupId";
    public static final String BOOTSTRAP_SERVERS = "bootstrapServers";

    @Test
    public void testBaseConfiguration() {
        final JsonObject config = makeBaseConfiguration();

        final int instanceId = 1;
        final KafkaConsumerConfiguration kafkaConsumerConfiguration = new KafkaConsumerVerticle().createKafkaConsumerConfiguration(config, instanceId);

        assertThat(kafkaConsumerConfiguration.getKafkaTopic(), is(TOPIC));
        assertThat(kafkaConsumerConfiguration.getClientId(), is(CLIENT_ID + "-" + instanceId));
        assertThat(kafkaConsumerConfiguration.getGroupId(), is(GROUP_ID));
        assertThat(kafkaConsumerConfiguration.getBootstrapServers(), is(BOOTSTRAP_SERVERS));
    }

    @Test
    public void testMessagesPerSecondDefault() {
        final JsonObject config = makeBaseConfiguration();

        final int instanceId = 1;
        final KafkaConsumerConfiguration kafkaConsumerConfiguration = new KafkaConsumerVerticle().createKafkaConsumerConfiguration(config, instanceId);

        assertThat(kafkaConsumerConfiguration.getMessagesPerSecond(), is(-1.0D));
    }

    @Test
    public void testMessagesPerSecondDefaultIsAppliedNonStrict() {
        final JsonObject config = makeBaseConfiguration()
                .put(KafkaConsumerProperties.KEY_STRICT_ORDERING, false);

        final int instanceId = 1;
        final KafkaConsumerConfiguration kafkaConsumerConfiguration = new KafkaConsumerVerticle().createKafkaConsumerConfiguration(config, instanceId);

        assertThat(kafkaConsumerConfiguration.getMessagesPerSecond(), is(KafkaConsumerVerticle.NON_STRICT_ORDERING_MESSAGES_PER_SECOND_DEFAULT));
    }

    @Test
    public void testMessagesPerSecondWrongValueAdaptsMaxPollInterval() {
        final JsonObject config = makeBaseConfiguration()
                .put(KafkaConsumerProperties.KEY_MESSAGES_PER_SECOND, 1.0D);

        final int instanceId = 1;
        final KafkaConsumerConfiguration kafkaConsumerConfiguration = new KafkaConsumerVerticle().createKafkaConsumerConfiguration(config, instanceId);

        assertThat(kafkaConsumerConfiguration.getMessagesPerSecond(), is(1.0D));
        assertThat(kafkaConsumerConfiguration.getMaxPollIntervalMs(), is(1000000L));
    }

    private JsonObject makeBaseConfiguration() {
        return new JsonObject()
                .put(KafkaConsumerProperties.KEY_CLIENT_ID, CLIENT_ID)
                .put(KafkaConsumerProperties.KEY_KAFKA_TOPIC, TOPIC)
                .put(KafkaConsumerProperties.KEY_GROUP_ID, GROUP_ID)
                .put(KafkaConsumerProperties.KEY_BOOTSTRAP_SERVERS, BOOTSTRAP_SERVERS);
    }

}
