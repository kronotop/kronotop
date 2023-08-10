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

package com.kronotop.core.cluster.journal;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.MutationType;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.kronotop.common.utils.ByteUtils;
import com.kronotop.common.utils.DirectoryLayout;
import com.kronotop.core.Context;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;

public class Journal {
    private final Context context;
    private final Subspace journal;
    private final byte[] INDEX_KEY;
    private final byte[] BROADCAST_KEY;

    public Journal(Context context) {
        this.context = context;
        this.journal = context.getFoundationDB().run(tr -> {
            List<String> subpath = DirectoryLayout.
                    Builder.
                    clusterName(context.getClusterName()).
                    internal().
                    cluster().asList();
            DirectorySubspace directorySubspace = DirectoryLayer.getDefault().createOrOpen(tr, subpath).join();
            return directorySubspace.subspace(Tuple.from("J"));
        });
        this.BROADCAST_KEY = journal.subspace(Tuple.from("B")).pack();
        this.INDEX_KEY = journal.subspace(Tuple.from("I")).pack();
    }

    public byte[] getBroadcastKey() {
        return BROADCAST_KEY;
    }

    public long publish(byte[] value) {
        return context.getFoundationDB().run((Transaction tr) -> {
            long offset = lastIndex(tr);
            tr.set(journal.subspace(Tuple.from(offset, "K")).pack(), value);
            tr.mutate(MutationType.ADD, BROADCAST_KEY, ByteUtils.fromLong(1L));
            return offset;
        });
    }

    public JournalItem consume(long offset) {
        return context.getFoundationDB().run((Transaction tr) -> {
            final KeyValue item = firstItem(tr, offset);
            if (item == null)
                return null;

            long itemOffset = (long) Tuple.fromBytes(item.getKey()).get(2);
            return new JournalItem(itemOffset, item.getValue());
        });
    }

    private KeyValue firstItem(Transaction tr, long offset) {
        if (offset == 0) {
            throw new IllegalArgumentException("0 is an illegal offset");
        }
        for (KeyValue kv : tr.getRange(journal.range(Tuple.from(offset)), 1)) {
            return kv;
        }
        return null;
    }

    private long lastIndex(Transaction tr) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(8);
        byteBuffer.order(ByteOrder.LITTLE_ENDIAN);
        byteBuffer.putLong(1L);

        tr.mutate(MutationType.ADD, INDEX_KEY, byteBuffer.array());

        byte[] value = tr.get(INDEX_KEY).join();
        byteBuffer.clear();
        byteBuffer.order(ByteOrder.LITTLE_ENDIAN);
        byteBuffer.put(value);
        return byteBuffer.getLong(0);
    }

    public long getLastIndex() {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            byte[] value = tr.snapshot().get(INDEX_KEY).join();
            if (value == null) {
                return 0;
            }
            ByteBuffer byteBuffer = ByteBuffer.allocate(8);
            byteBuffer.order(ByteOrder.LITTLE_ENDIAN);
            byteBuffer.put(value);
            return byteBuffer.getLong(0);
        }
    }
}
