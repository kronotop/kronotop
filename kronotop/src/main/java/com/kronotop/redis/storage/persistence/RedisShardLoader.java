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

package com.kronotop.redis.storage.persistence;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.async.AsyncIterable;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.directory.NoSuchDirectoryException;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.kronotop.Context;
import com.kronotop.redis.hash.HashFieldValue;
import com.kronotop.redis.hash.HashValue;
import com.kronotop.redis.storage.RedisShard;
import com.kronotop.server.WrongTypeException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletionException;

/**
 * The RedisShardLoader class is responsible for loading data from a specified directory subspace into a Shard object.
 * It provides methods to load string values and hash values from the subspace into the Shard.
 */
public final class RedisShardLoader {
    private final Context context;
    private final RedisShard shard;
    private Range range;

    public RedisShardLoader(Context context, RedisShard shard) {
        this.context = context;
        this.shard = shard;
    }

    /**
     * Loads the hash values from a given hash subspace.
     *
     * @param tr       the transaction used to access the database
     * @param subspace the hash subspace from which hash values are loaded
     */
    private void loadHashValue(Transaction tr, DirectorySubspace subspace) {
        if (range == null) {
            range = new Range(subspace.pack(), ByteArrayUtil.strinc(subspace.pack()));
        }

        String key = subspace.getPath().getLast();
        HashValue hashValue;
        RedisValueContainer container = shard.storage().get(key);
        if (container == null) {
            hashValue = new HashValue();
            shard.storage().put(key, new RedisValueContainer(hashValue));
        } else {
            if (!(container.kind().equals(RedisValueKind.HASH))) {
                // TODO: add key to the error message
                throw new WrongTypeException();
            }
            hashValue = container.hash();
        }
        AsyncIterable<KeyValue> asyncIterable = tr.snapshot().getRange(range);
        for (KeyValue keyValue : asyncIterable) {
            range = new Range(keyValue.getKey(), range.end);
            String field = subspace.unpack(keyValue.getKey()).get(0).toString();
            hashValue.put(field, new HashFieldValue(keyValue.getValue()));
        }
    }

    /**
     * Loads hash values from a given hash subspace.
     *
     * @param tr           the transaction used to access the database
     * @param hashSubspace the hash subspace from which hash values are loaded
     */
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

    /**
     * Loads string values from a given subspace in the database.
     *
     * @param tr       the transaction used to access the database
     * @param subspace the subspace from which string values are loaded
     */
    private void loadStringValues(Transaction tr, DirectorySubspace subspace) {
        if (range == null) {
            range = new Range(subspace.pack(), ByteArrayUtil.strinc(subspace.pack()));
        }

        AsyncIterable<KeyValue> asyncIterable = tr.snapshot().getRange(range);
        for (KeyValue keyValue : asyncIterable) {
            range = new Range(keyValue.getKey(), range.end);
            try {
                ByteBuffer data = ByteBuffer.wrap(keyValue.getValue());
                StringPack stringPack = StringPack.unpack(data);
                String key = subspace.unpack(keyValue.getKey()).get(0).toString();
                shard.storage().computeIfAbsent(key, (k) -> {
                    shard.index().add(k);
                    shard.index().flush();
                    return new RedisValueContainer(stringPack.stringValue());
                });
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * Loads values from a given subspace in the database.
     *
     * @param tr            the transaction used to access the database
     * @param dataStructure the data structure from which values are loaded
     */
    public void load(Transaction tr, DataStructure dataStructure) {
        try {
            DirectorySubspace subspace = context.getDirectoryLayer().openDataStructure(shard.id(), dataStructure);
            if (dataStructure.equals(DataStructure.STRING)) {
                loadStringValues(tr, subspace);
            } else if (dataStructure.equals(DataStructure.HASH)) {
                loadHashValues(tr, subspace);
            }
        } catch (CompletionException e) {
            if (e.getCause() instanceof NoSuchDirectoryException) {
                return;
            }
            throw new RuntimeException(e);
        }
        range = null;
    }
}