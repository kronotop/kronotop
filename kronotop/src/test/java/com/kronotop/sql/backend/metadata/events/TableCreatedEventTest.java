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

package com.kronotop.sql.backend.metadata.events;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class TableCreatedEventTest {
    @Test
    public void encode_decode_success() throws JsonProcessingException {
        List<String> schema = List.of("foobar", "barfoo");
        String table = "mytable";
        byte[] versionstamp = new byte[]{1, 2, 3};
        TableCreatedEvent schemaCreatedEvent = new TableCreatedEvent(schema, table, versionstamp);

        ObjectMapper objectMapper = new ObjectMapper();
        String encoded = objectMapper.writeValueAsString(schemaCreatedEvent);
        TableCreatedEvent decoded = objectMapper.readValue(encoded, TableCreatedEvent.class);
        assertThat(schemaCreatedEvent).usingRecursiveComparison().isEqualTo(decoded);
    }
}
