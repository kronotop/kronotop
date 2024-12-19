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

package com.kronotop.cluster;

import com.kronotop.JSONUtils;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class MemberLeftEventTest {

    @Test
    public void test_check_kind() {
        MemberLeftEvent event = new MemberLeftEvent(MemberIdGenerator.generateId());
        byte[] data = JSONUtils.writeValueAsBytes(event);
        assertNotNull(data);

        BaseBroadcastEvent base = JSONUtils.readValue(data, BaseBroadcastEvent.class);
        assertEquals(BroadcastEventKind.MEMBER_LEFT, base.kind());
    }

    @Test
    public void test_encode_then_decode() {
        MemberLeftEvent expected = new MemberLeftEvent(MemberIdGenerator.generateId());
        byte[] data = JSONUtils.writeValueAsBytes(expected);
        assertNotNull(data);

        MemberLeftEvent result = JSONUtils.readValue(data, MemberLeftEvent.class);
        assertThat(expected).usingRecursiveComparison().isEqualTo(result);
    }
}
