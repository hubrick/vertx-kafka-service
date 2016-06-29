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

import com.hubrick.vertx.kafka.producer.model.ByteKafkaMessage;
import com.hubrick.vertx.kafka.producer.property.KafkaProducerProperties;
import io.vertx.core.AsyncResult;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Test;
import org.junit.runner.RunWith;


/**
 * Tests mod-kafka module with byte array serializer configuration.
 */
@RunWith(VertxUnitRunner.class)
public class ByteArraySerializerIntegrationTest extends AbstractVertxTest {

    private static final String ADDRESS = "default-address";
    private static final String MESSAGE = "Test bytes message!";
    private static final String TOPIC = "some-topic";

    @Test
    public void test(TestContext testContext) throws Exception {
        JsonObject config = new JsonObject();
        config.put(KafkaProducerProperties.ADDRESS, ADDRESS);
        config.put(KafkaProducerProperties.BROKER_LIST, KafkaProducerProperties.BROKER_LIST_DEFAULT);
        config.put(KafkaProducerProperties.DEFAULT_TOPIC, TOPIC);
        config.put(KafkaProducerProperties.REQUEST_ACKS, KafkaProducerProperties.REQUEST_ACKS_DEFAULT);

        final DeploymentOptions deploymentOptions = new DeploymentOptions();
        deploymentOptions.setConfig(config);
        deploy(testContext, deploymentOptions);

        final Async async = testContext.async();
        try {
            final KafkaProducerService kafkaProducerService = KafkaProducerService.createProxy(vertx, ADDRESS);
            kafkaProducerService.sendBytes(new ByteKafkaMessage(Buffer.buffer(MESSAGE.getBytes())), (Handler<AsyncResult<Void>>) message -> {
                if (message.failed()) {
                    testContext.assertTrue(message.cause().getMessage().equals("Failed to send messages after 3 tries."));
                    async.complete();
                } else {
                    testContext.fail();
                }
            });
        } catch (Exception e) {
            testContext.fail(e);
        }
    }
}
