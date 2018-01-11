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

import com.hubrick.vertx.kafka.consumer.property.KafkaConsumerProperties;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import org.junit.Test;

public class KafkaConsumerDeployWithIncorrectConfigIntegrationTest extends AbstractConsumerTest {

    @Test
    public void testMissingAddressFailure(TestContext testContext) throws Exception {
        final JsonObject config = makeDefaultConfig();
        config.put(KafkaConsumerProperties.KEY_VERTX_ADDRESS, (String) null);

        final Async async = testContext.async();
        final DeploymentOptions deploymentOptions = new DeploymentOptions();
        deploymentOptions.setConfig(config);
        vertx.deployVerticle(SERVICE_NAME, deploymentOptions, asyncResult -> {
            testContext.assertTrue(asyncResult.failed());
            testContext.assertNotNull("DeploymentID should not be null", asyncResult.result());
            testContext.assertEquals("No configuration for key address found", asyncResult.cause().getMessage());
            async.complete();
        });
    }

    @Test
    public void testUnresolvableBoostrapUrl(TestContext testContext) throws Exception {
        final JsonObject config = makeDefaultConfig();
        config.put(KafkaConsumerProperties.KEY_BOOTSTRAP_SERVERS, "someUnresolvableUrl.fail:9092");
        final Async async = testContext.async();
        final DeploymentOptions deploymentOptions = new DeploymentOptions();
        deploymentOptions.setConfig(config);
        vertx.deployVerticle(SERVICE_NAME, deploymentOptions, asyncResult -> {
            testContext.assertTrue(asyncResult.failed());
            testContext.assertNotNull("DeploymentID should not be null", asyncResult.result());
            testContext.assertEquals("Failed to construct kafka consumer", asyncResult.cause().getMessage());
            async.complete();
        });
    }
}
