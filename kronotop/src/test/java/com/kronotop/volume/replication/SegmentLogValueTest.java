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

package com.kronotop.volume.replication;

import com.kronotop.volume.OperationKind;
import com.kronotop.volume.Prefix;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SegmentLogValueTest {

    @Test
    public void test_SegmentLogValue() {
        Prefix prefix = new Prefix("some-prefix");
        SegmentLogValue value = new SegmentLogValue(OperationKind.APPEND, prefix.asLong(), 0, 100);
        ByteBuffer buffer = value.encode();
        SegmentLogValue decoded = SegmentLogValue.decode(buffer);
        assertEquals(value, decoded);
    }
}