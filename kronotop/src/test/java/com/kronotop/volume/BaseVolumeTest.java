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

package com.kronotop.volume;

import com.apple.foundationdb.Transaction;
import com.google.common.base.Strings;
import com.kronotop.BaseMetadataStoreTest;

import java.nio.ByteBuffer;
import java.util.Random;

public class BaseVolumeTest extends BaseMetadataStoreTest {
    private final int entryLength = 10;
    protected Prefix prefix = new Prefix("test-prefix".getBytes());
    Random random = new Random();

    long getReadVersion() {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            return tr.getReadVersion().join();
        }
    }

    protected ByteBuffer randomBytes(int size) {
        byte[] b = new byte[size];
        random.nextBytes(b);
        return ByteBuffer.wrap(b);
    }

    public ByteBuffer getEntry() {
        byte[] data = new byte[10];
        new Random().nextBytes(data);
        return ByteBuffer.allocate(entryLength).put(data).flip();
    }

    public ByteBuffer[] getEntries(int number) {
        ByteBuffer[] entries = new ByteBuffer[number];
        for (int i = 0; i < number; i++) {
            byte[] data = Strings.padStart(Integer.toString(i), entryLength, '0').getBytes();
            entries[i] = ByteBuffer.allocate(entryLength).put(data).flip();
        }
        return entries;
    }
}