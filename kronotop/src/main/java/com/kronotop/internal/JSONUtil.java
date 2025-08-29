/*
 * Copyright (c) 2023-2025 Burak Sezer
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

package com.kronotop.internal;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kronotop.KronotopException;

import java.io.IOException;

/**
 * The JSONUtils class provides utility methods for reading and writing JSON data using the Jackson ObjectMapper.
 */
public class JSONUtil {
    public static final ObjectMapper objectMapper = new ObjectMapper();

    public static <T> T readValue(byte[] content, Class<T> valueType) {
        if (content == null) {
            return null;
        }
        try {
            return objectMapper.readValue(content, valueType);
        } catch (IOException e) {
            throw new KronotopException("JSON deserialization failed", e);
        }
    }

    public static byte[] writeValueAsBytes(Object value) {
        try {
            return objectMapper.writeValueAsBytes(value);
        } catch (JsonProcessingException e) {
            throw new KronotopException("JSON serialization failed", e);
        }
    }
}
