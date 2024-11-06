/*
 * Copyright (c) 2023-2024 Kronotop
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

package com.kronotop.journal;

import java.nio.ByteBuffer;

/**
 * The Entry record represents an event with a timestamp.
 */
record Entry(byte[] event, Long timestamp) {

    /**
     * Decodes a ByteBuffer into an Entry.
     *
     * @param buffer The ByteBuffer containing the encoded entry with a timestamp.
     * @return The decoded Entry containing the event data and timestamp.
     */
    static Entry decode(ByteBuffer buffer) {
        long timestamp = buffer.getLong();
        int remaining = buffer.remaining();
        byte[] value = new byte[remaining];
        buffer.get(value);
        return new Entry(value, timestamp);
    }

    /**
     * Encodes the event and its length into a byte array.
     *
     * @return A byte array containing the size of the event followed by the event data.
     */
    byte[] encode() {
        ByteBuffer buffer = ByteBuffer.allocate(event.length + 8);
        buffer.putLong(timestamp);
        buffer.put(event);
        buffer.flip();
        return buffer.array();
    }
}
