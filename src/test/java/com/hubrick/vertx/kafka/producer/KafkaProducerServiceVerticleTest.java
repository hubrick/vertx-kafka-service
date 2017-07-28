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
package com.hubrick.vertx.kafka.producer;


import com.hubrick.vertx.kafka.producer.config.KafkaProducerConfiguration;
import com.hubrick.vertx.kafka.producer.model.KafkaOptions;
import com.hubrick.vertx.kafka.producer.model.StringKafkaMessage;
import com.hubrick.vertx.kafka.producer.property.KafkaProducerProperties;
import com.timgroup.statsd.StatsDClient;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Map;

import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class KafkaProducerServiceVerticleTest {

    @Mock
    private Map<MessageSerializerType, Producer> producers;

    @Mock
    private Producer<String, String> producer;

    @Mock
    private Logger logger;

    @Mock
    private Message<JsonObject> event;

    @Spy
    private KafkaProducerConfiguration kafkaProducerConfiguration = new KafkaProducerConfiguration(
            "default-topic",
            "localhost:9092",
            "1",
            KafkaProducerProperties.RETRIES_DEFAULT,
            KafkaProducerProperties.REQUEST_TIMEOUT_MS_DEFAULT,
            KafkaProducerProperties.MAX_BLOCK_MS_DEFAULT,
            null,
            null);

    @Mock
    private StatsDClient statsDClient;

    @InjectMocks
    private DefaultKafkaProducerService kafkaMessageProducer = new DefaultKafkaProducerService(kafkaProducerConfiguration);

    @Before
    public void setUp() throws Exception {
        when(producers.get(MessageSerializerType.STRING_SERIALIZER)).thenReturn(producer);
    }

    @Test
    public void sendMessageToKafka() {
        kafkaMessageProducer.sendString(new StringKafkaMessage("test payload"), event1 -> {
            verify(producer, times(1)).send(new ProducerRecord("default-topic", null, "test payload"));
        });
    }

    @Test
    public void sendMessageToKafkaWithPartKey() {
        kafkaMessageProducer.sendString(new StringKafkaMessage("test payload", "some partition"), event1 -> {
            verify(producer, times(1)).send(new ProducerRecord("default-topic", "some partition", "test payload"));
        });
    }

    @Test
    public void sendMessageToKafkaWithTopic() {
        kafkaMessageProducer.sendString(new StringKafkaMessage("test payload", "some partition"), new KafkaOptions().setTopic("foo-topic"), event1 -> {
            verify(producer, times(1)).send(new ProducerRecord("foo-topic", "some partition", "test payload"));
        });
    }

    @Test
    public void sendMessageToKafkaVerifyStatsDExecutorCalled() {
        kafkaMessageProducer.sendString(new StringKafkaMessage("test payload"), event1 -> {
            verify(producer, times(1)).send(new ProducerRecord("default-topic", null, "test payload"));
            verify(statsDClient, times(1)).recordExecutionTime(anyString(), anyLong());
        });

    }

}
