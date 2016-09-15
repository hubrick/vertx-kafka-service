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

import java.util.HashMap;
import java.util.Map;

/**
 * @author Emir Dizdarevic
 * @since 1.2.0
 */
public enum ProducerType {
    SYNC,
    ASYNC;

    private static final Map<String, ProducerType> REVERSE_LOOKUP = new HashMap<>();
    static {
        for(ProducerType producerType : values()) {
            REVERSE_LOOKUP.put(producerType.name().toLowerCase(), producerType);
        }
    }

    public String getValue() {
        return name().toLowerCase();
    }

    public static ProducerType fromString(String value) {
        return REVERSE_LOOKUP.get(value.toLowerCase());
    }
}
