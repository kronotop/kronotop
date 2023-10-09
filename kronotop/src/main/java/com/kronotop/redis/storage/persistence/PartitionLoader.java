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

package com.kronotop.redis.storage.persistence;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.async.AsyncIterable;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.directory.NoSuchDirectoryException;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.kronotop.common.utils.DirectoryLayout;
import com.kronotop.core.Context;
import com.kronotop.redis.HashValue;
import com.kronotop.redis.StringValue;
import com.kronotop.redis.storage.LogicalDatabase;
import com.kronotop.redis.storage.Partition;
import com.kronotop.server.resp.WrongTypeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletionException;

public final class PartitionLoader {
    private static final Logger logger = LoggerFactory.getLogger(PartitionLoader.class);
    private final DirectoryLayer directoryLayer = new DirectoryLayer();
    private final List<String> rootPath;
    private final Partition partition;
    private Range range;

    public PartitionLoader(Context context, Partition partition) {
        this.partition = partition;
        this.rootPath = DirectoryLayout.Builder.clusterName(context.getClusterName()).internal().redis().persistence().asList();
    }

    private void loadHashValue(Transaction tr, DirectorySubspace subspace) {
        if (range == null) {
            range = new Range(subspace.pack(), ByteArrayUtil.strinc(subspace.pack()));
        }

        String key = subspace.getPath().get(subspace.getPath().size() - 1);
        HashValue hashValue;
        Object retrieved = partition.get(key);
        if (retrieved == null) {
            hashValue = new HashValue();
            partition.put(key, hashValue);
        } else {
            if (!(retrieved instanceof HashValue)) {
                // TODO: add key to the error message
                throw new WrongTypeException();
            }
            hashValue = (HashValue) retrieved;
        }
        AsyncIterable<KeyValue> asyncIterable = tr.snapshot().getRange(range);
        for (KeyValue keyValue : asyncIterable) {
            range = new Range(keyValue.getKey(), range.end);
            String field = subspace.unpack(keyValue.getKey()).get(0).toString();
            hashValue.put(field, keyValue.getValue());
        }
    }

    private void loadHashValues(Transaction tr, DirectorySubspace hashSubspace) {
        List<DirectorySubspace> subspaces = new ArrayList<>();
        List<String> hashes = hashSubspace.list(tr).join();
        for (String hash : hashes) {
            DirectorySubspace subspace = hashSubspace.open(tr, Collections.singletonList(hash)).join();
            subspaces.add(subspace);
        }
        for (DirectorySubspace subspace : subspaces) {
            loadHashValue(tr, subspace);
        }
    }

    private void loadStringValues(Transaction tr, DirectorySubspace subspace) {
        if (range == null) {
            range = new Range(subspace.pack(), ByteArrayUtil.strinc(subspace.pack()));
        }

        AsyncIterable<KeyValue> asyncIterable = tr.snapshot().getRange(range);
        for (KeyValue keyValue : asyncIterable) {
            range = new Range(keyValue.getKey(), range.end);
            try {
                StringValue stringValue = StringValue.decode(keyValue.getValue());
                String key = subspace.unpack(keyValue.getKey()).get(0).toString();
                partition.computeIfAbsent(key, (k) -> {
                    partition.getIndex().add(k);
                    partition.getIndex().flush();
                    return stringValue;
                });
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public void load(Transaction tr, DataStructure dataStructure) {
        List<String> subpath = new ArrayList<>(rootPath);
        subpath.add(LogicalDatabase.NAME);
        subpath.add(partition.getId().toString());
        subpath.add(dataStructure.name().toLowerCase());

        DirectorySubspace subspace;
        try {
            subspace = directoryLayer.open(tr, subpath).join();
        } catch (CompletionException e) {
            if (e.getCause() instanceof NoSuchDirectoryException) {
                return;
            }
            throw new RuntimeException(e);
        }
        if (dataStructure.equals(DataStructure.STRING)) {
            loadStringValues(tr, subspace);
        } else if (dataStructure.equals(DataStructure.HASH)) {
            loadHashValues(tr, subspace);
        }

        range = null;
    }
}