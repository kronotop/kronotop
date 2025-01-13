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

package com.kronotop.network;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;

import java.io.IOException;

public class AddressDeserializer extends StdDeserializer<Address> {

    public AddressDeserializer() {
        this(null);
    }

    public AddressDeserializer(final Class<?> vc) {
        super(vc);
    }

    @Override
    public Address deserialize(final JsonParser parser, final DeserializationContext context) throws IOException {
        JsonNode node = parser.getCodec().readTree(parser);
        String host = node.get("host").textValue();
        int port = node.get("port").asInt();
        return new Address(host, port);
    }
}