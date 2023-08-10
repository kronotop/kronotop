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

import com.apple.foundationdb.FDBException;
import com.apple.foundationdb.KeySelector;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.async.AsyncIterable;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.directory.NoSuchDirectoryException;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.kronotop.common.resp.RESPError;
import com.kronotop.common.utils.DirectoryLayout;
import com.kronotop.core.Context;
import com.kronotop.redis.HashValue;
import com.kronotop.redis.StringValue;
import com.kronotop.redis.storage.LogicalDatabase;
import com.kronotop.server.resp.WrongTypeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

public final class DataStructureLoader {
    private static final Logger logger = LoggerFactory.getLogger(DataStructureLoader.class);
    private final Context context;
    private final DirectoryLayer directoryLayer = new DirectoryLayer();
    private final List<String> rootPath;

    public DataStructureLoader(Context context) {
        this.context = context;
        this.rootPath = DirectoryLayout.Builder.clusterName(context.getClusterName()).internal().redis().persistence().asList();
    }

    private void loadHashValue(LogicalDatabase logicalDatabase, DirectorySubspace subspace) {
        byte[] begin = subspace.pack();
        byte[] end = ByteArrayUtil.strinc(subspace.pack());
        String key = subspace.getPath().get(subspace.getPath().size() - 1);

        HashValue hashValue;
        Object retrieved = logicalDatabase.get(key);
        if (retrieved == null) {
            hashValue = new HashValue();
            logicalDatabase.put(key, hashValue);
        } else {
            if (!(retrieved instanceof HashValue)) {
                // TODO: add key to the error message
                throw new WrongTypeException(RESPError.WRONGTYPE_MESSAGE);
            }
            hashValue = (HashValue) retrieved;
        }

        while (true) {
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                AsyncIterable<KeyValue> asyncIterable = tr.snapshot().getRange(KeySelector.firstGreaterOrEqual(begin), KeySelector.firstGreaterOrEqual(end));
                for (KeyValue keyValue : asyncIterable) {
                    begin = keyValue.getKey();
                    String field = subspace.unpack(keyValue.getKey()).get(0).toString();
                    hashValue.put(field, keyValue.getValue());
                }
                break;
            } catch (CompletionException e) {
                if (e.getCause() instanceof FDBException) {
                    String message = RESPError.decapitalize(e.getCause().getMessage());
                    if (message.equalsIgnoreCase(RESPError.TRANSACTION_TOO_OLD_MESSAGE)) {
                        continue;
                    }
                }
                throw e;
            }
        }
    }

    private void loadHashValues(LogicalDatabase logicalDatabase, DirectorySubspace hashSubspace) {
        List<DirectorySubspace> subspaces = new ArrayList<>();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            List<String> hashes = hashSubspace.list(tr).join();
            for (String hash : hashes) {
                DirectorySubspace subspace = hashSubspace.open(tr, Collections.singletonList(hash)).join();
                subspaces.add(subspace);
            }
        }
        for (DirectorySubspace subspace : subspaces) {
            loadHashValue(logicalDatabase, subspace);
        }
    }

    private void loadStringValues(LogicalDatabase logicalDatabase, DirectorySubspace subspace) {
        byte[] begin = subspace.pack();
        byte[] end = ByteArrayUtil.strinc(subspace.pack());

        while (true) {
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                AsyncIterable<KeyValue> asyncIterable = tr.snapshot().getRange(KeySelector.firstGreaterOrEqual(begin), KeySelector.firstGreaterOrEqual(end));
                for (KeyValue keyValue : asyncIterable) {
                    begin = keyValue.getKey();
                    try {
                        StringValue stringValue = StringValue.decode(keyValue.getValue());
                        String key = subspace.unpack(keyValue.getKey()).get(0).toString();
                        logicalDatabase.put(key, stringValue);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
                break;
            } catch (CompletionException e) {
                if (e.getCause() instanceof FDBException) {
                    String message = RESPError.decapitalize(e.getCause().getMessage());
                    if (message.equalsIgnoreCase(RESPError.TRANSACTION_TOO_OLD_MESSAGE)) {
                        continue;
                    }
                }
                throw e;
            }
        }
    }

    public void load(LogicalDatabase logicalDatabase, DataStructure dataStructure) {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            CompletableFuture<List<String>> future = directoryLayer.list(tr, rootPath);
            List<String> logicalDatabaseIndexes;
            try {
                logicalDatabaseIndexes = future.join();
            } catch (CompletionException e) {
                if (e.getCause() instanceof NoSuchDirectoryException) {
                    logger.debug("No such directory: {}", String.join(".", rootPath));
                    return;
                }
                throw new RuntimeException(e);
            }

            for (String index : logicalDatabaseIndexes) {
                List<String> subpath = new ArrayList<>(rootPath);
                subpath.add(index);
                subpath.add(dataStructure.name().toLowerCase());

                DirectorySubspace subspace;
                try {
                    subspace = directoryLayer.open(tr, subpath).join();
                } catch (CompletionException e) {
                    if (e.getCause() instanceof NoSuchDirectoryException) {
                        logger.debug("No such directory: {}", String.join(".", subpath));
                        return;
                    }
                    throw new RuntimeException(e);
                }
                if (dataStructure.equals(DataStructure.STRING)) {
                    loadStringValues(logicalDatabase, subspace);
                } else if (dataStructure.equals(DataStructure.HASH)) {
                    loadHashValues(logicalDatabase, subspace);
                }
            }
        }
    }
}