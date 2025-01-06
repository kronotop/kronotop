/*
 * Copyright (c) 2023-2025 Kronotop
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.kronotop;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kronotop.common.KronotopException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * The JSONUtils class provides utility methods for reading and writing JSON data using the Jackson ObjectMapper.
 */
public class JSONUtils {
    public static final ObjectMapper objectMapper = new ObjectMapper();
    private static final Logger LOGGER = LoggerFactory.getLogger(JSONUtils.class);

    public static <T> T readValue(byte[] content, Class<T> valueType) {
        try {
            return objectMapper.readValue(content, valueType);
        } catch (IOException e) {
            LOGGER.error("Failed to deserialize JSON content from given resource into given Java type. ", e);
            throw new KronotopException("JSON deserialization failed");
        }
    }

    public static byte[] writeValueAsBytes(Object value) {
        try {
            return objectMapper.writeValueAsBytes(value);
        } catch (JsonProcessingException e) {
            LOGGER.error("Failed to serialize Java object to byte array. ", e);
            throw new KronotopException("JSON serialization failed");
        }
    }
}
