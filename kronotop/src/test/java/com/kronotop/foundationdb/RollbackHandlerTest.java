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

package com.kronotop.foundationdb;

import com.kronotop.server.resp3.ErrorRedisMessage;
import com.kronotop.server.resp3.SimpleStringRedisMessage;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

public class RollbackHandlerTest extends BaseHandlerTest {
    @Test
    public void testROLLBACK() {
        TestTransaction tt = new TestTransaction(channel);
        tt.begin();
        tt.cancel();
        Object response = tt.getResponse();

        assertInstanceOf(SimpleStringRedisMessage.class, response);
        SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) response;
        assertEquals("OK", actualMessage.content());
    }

    @Test
    public void testROLLBACK_NoTransactionInProgress() {
        TestTransaction tt = new TestTransaction(channel);

        // Start a new transaction
        tt.begin();
        tt.cancel();
        tt.cancel();

        Object response = tt.getResponse();
        assertInstanceOf(ErrorRedisMessage.class, response);
        ErrorRedisMessage actualMessage = (ErrorRedisMessage) response;
        assertEquals("TRANSACTION there is no transaction in progress.", actualMessage.content());
    }
}