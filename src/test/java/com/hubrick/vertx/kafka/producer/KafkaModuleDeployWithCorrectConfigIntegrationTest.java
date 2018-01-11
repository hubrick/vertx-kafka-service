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

import com.hubrick.vertx.kafka.producer.model.StringKafkaMessage;
import com.hubrick.vertx.kafka.producer.property.KafkaProducerProperties;
import io.vertx.core.AsyncResult;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 * Tests mod-kafka module specifying correct configuration with all required parameters.
 * <p/>
 * This test sends an event to Vert.x EventBus, then registers a handler to handle that event
 * and send it to Kafka broker, by creating Kafka Producer. It checks that the flow works correctly
 * until the point, where message is sent to Kafka.
 */
@RunWith(VertxUnitRunner.class)
public class KafkaModuleDeployWithCorrectConfigIntegrationTest extends AbstractProducerTest {

    private static final String ADDRESS = "default-address";
    private static final String MESSAGE = "Test message from KafkaModuleDeployWithCorrectConfigIT!";
    private static final String TOPIC = "some-topic";

    @Test
    public void test(TestContext testContext) throws Exception {
        JsonObject config = makeDefaultConfig();
        config.put(KafkaProducerProperties.ADDRESS, ADDRESS);
        config.put(KafkaProducerProperties.DEFAULT_TOPIC, TOPIC);

        final DeploymentOptions deploymentOptions = new DeploymentOptions();
        deploymentOptions.setConfig(config);
        deploy(testContext, deploymentOptions);

        final Async async = testContext.async();
        try {
            sendMessage(testContext, async);
        } catch (Exception e) {
            testContext.fail(e);
        }
    }

    public void sendMessage(TestContext testContext, Async async) throws Exception {
        final KafkaProducerService kafkaProducerService = KafkaProducerService.createProxy(vertx, ADDRESS);
        kafkaProducerService.sendString(new StringKafkaMessage(MESSAGE), (Handler<AsyncResult<Void>>) message -> {
            if (message.failed()) {
                testContext.assertTrue(message.cause().getMessage().equals("Failed to update metadata after 5 ms."));
                async.complete();
            } else {
                testContext.fail();
            }
        });
    }
}
