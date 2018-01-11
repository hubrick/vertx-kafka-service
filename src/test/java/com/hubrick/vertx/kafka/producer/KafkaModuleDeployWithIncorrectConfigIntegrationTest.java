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

import com.hubrick.vertx.kafka.producer.property.KafkaProducerProperties;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 * Tests mod-kafka module specifying incorrect configuration with missing required parameter.
 * Then verifies that deployment of module fails with a message, that missing parameter should be specified.
 */
@RunWith(VertxUnitRunner.class)
public class KafkaModuleDeployWithIncorrectConfigIntegrationTest extends AbstractProducerTest {

    private static final String TOPIC = "some-topic";

    @Test
    // The test should fail to start the deployment
    public void sendMessage(TestContext testContext) throws Exception {

        JsonObject config =  makeDefaultConfig();
        config.put(KafkaProducerProperties.DEFAULT_TOPIC, TOPIC);

        final Async async = testContext.async();
        final DeploymentOptions deploymentOptions = new DeploymentOptions();
        deploymentOptions.setConfig(config);
        vertx.deployVerticle(SERVICE_NAME, deploymentOptions, asyncResult -> {
            testContext.assertTrue(asyncResult.failed());
            testContext.assertNotNull("DeploymentID should not be null", asyncResult.result());
            testContext.assertEquals("address must be specified in config", asyncResult.cause().getMessage());
            async.complete();
        });
    }
}
