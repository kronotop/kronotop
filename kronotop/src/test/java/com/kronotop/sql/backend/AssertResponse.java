/*
 * Copyright (c) 2023 Kronotop
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

package com.kronotop.sql.backend;

import com.kronotop.server.resp3.ArrayRedisMessage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

public class AssertResponse<T> {

    @SuppressWarnings("unchecked")
    public T getMessage(Object response, int index, int size) {
        assertInstanceOf(ArrayRedisMessage.class, response);
        ArrayRedisMessage actualMessage = (ArrayRedisMessage) response;
        assertEquals(size, actualMessage.children().size());
        return (T) actualMessage.children().get(index);
    }
}
