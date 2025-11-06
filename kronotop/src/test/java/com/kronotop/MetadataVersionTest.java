/*
 * Copyright (c) 2023-2025 Burak Sezer
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.kronotop;

import com.kronotop.internal.TransactionUtils;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class MetadataVersionTest extends BaseStandaloneInstanceTest {

    @Test
    void shouldSetMetadataVersionDuringInitialization() {
        String version = TransactionUtils.execute(context, tr -> MetadataVersion.read(context, tr));
        assertEquals(MetadataVersion.CURRENT, version);
    }

    @Test
    void shouldOverwriteMetadataVersion() {
        TransactionUtils.executeThenCommit(context, tr -> {
            MetadataVersion.write(context, tr, "2.0.0");
            return null;
        });

        String version = TransactionUtils.execute(context, tr -> MetadataVersion.read(context, tr));
        assertEquals("2.0.0", version);
    }
}