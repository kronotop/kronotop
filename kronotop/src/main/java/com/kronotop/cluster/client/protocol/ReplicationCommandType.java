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

package com.kronotop.cluster.client.protocol;

import io.lettuce.core.protocol.ProtocolKeyword;

import java.nio.charset.StandardCharsets;

public enum ReplicationCommandType implements ProtocolKeyword {
    SEGMENTRANGE("SEGMENT.RANGE"),
    SEGMENTINSERT("SEGMENT.INSERT"),
    CHANGELOGWATCH("CHANGELOG.WATCH"),
    SEGMENTTAILPOINTER("SEGMENT.TAILPOINTER"),
    CHANGELOGRANGE("CHANGELOG.RANGE");

    public final byte[] bytes;

    ReplicationCommandType(String name) {
        bytes = name.getBytes(StandardCharsets.US_ASCII);
    }

    @Override
    public byte[] getBytes() {
        return bytes;
    }
}
