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

package com.kronotop.volume;

import com.apple.foundationdb.Transaction;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ReplicationTest extends BaseVolumeIntegrationTest {
    @Test
    public void test_replication() throws IOException {

        {
            ByteBuffer[] entries = getEntries(10);
            AppendResult result;
            try (Transaction tr = database.createTransaction()) {
                Session session = new Session(tr);
                result = volume.append(session, entries);
                tr.commit().join();
            }
            assertEquals(10, result.getVersionstampedKeys().length);
        }


        Replication replication = new Replication(context, volume);
        replication.start();
    }
}