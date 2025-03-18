/*
 * Copyright (c) 2023-2025 Burak Sezer
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

import com.apple.foundationdb.MutationType;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.kronotop.common.KronotopException;

import javax.annotation.Nonnull;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * SegmentMetadata handles the caching and retrieval of segment metadata information
 * like cardinality and used bytes. It utilizes a LoadingCache to cache Key
 * objects that represent specific segment metadata.
 */
class SegmentMetadata {
    private static final byte[] INCREASE_BY_ONE_DELTA = new byte[]{1, 0, 0, 0}; // 1, byte order: little-endian
    private static final byte[] DECREASE_BY_ONE_DELTA = new byte[]{-1, -1, -1, -1}; // -1, byte order: little-endian

    private final LoadingCache<Prefix, Key> keys;

    SegmentMetadata(VolumeSubspace subspace, String name) {
        this.keys = CacheBuilder.newBuilder()
                .expireAfterAccess(10, TimeUnit.MINUTES)
                .build(new KeysLoader(subspace, name));
    }

    /**
     * Increases the cardinality value associated with the given session by one.
     *
     * @param session The session object containing the current transaction and prefix information.
     */
    void increaseCardinalityByOne(VolumeSession session) {
        increaseCardinality(session, INCREASE_BY_ONE_DELTA);
    }

    /**
     * Decreases the cardinality value associated with the given session by one.
     *
     * @param session The session object containing the current transaction and prefix information.
     */
    void decreaseCardinalityByOne(VolumeSession session) {
        increaseCardinality(session, DECREASE_BY_ONE_DELTA);
    }

    /**
     * Increases the cardinality value associated with the given session by the specified delta.
     *
     * @param session The session object containing the current transaction and prefix information.
     * @param delta   The byte array representing the value to add to the cardinality.
     * @throws KronotopException if an execution exception occurs while performing the mutation.
     */
    void increaseCardinality(VolumeSession session, byte[] delta) {
        try {
            Key key = keys.get(session.prefix());
            session.transaction().mutate(MutationType.ADD, key.cardinality(), delta);
        } catch (ExecutionException e) {
            throw new KronotopException(e);
        }
    }

    /**
     * Resets the cardinality value associated with the given session by clearing it in the transaction.
     *
     * @param session The session object containing the current transaction and prefix information.
     * @throws KronotopException if an execution exception occurs while performing the operation.
     */
    void resetCardinality(VolumeSession session) {
        try {
            Key key = keys.get(session.prefix());
            session.transaction().clear(key.cardinality());
        } catch (ExecutionException e) {
            throw new KronotopException(e);
        }
    }

    /**
     * Retrieves the cardinality value associated with the given session.
     *
     * @param session The session object containing the current transaction and prefix information.
     * @return The cardinality value associated with the session.
     * @throws KronotopException if an execution exception occurs while retrieving the cardinality data.
     */
    int cardinality(VolumeSession session) {
        try {
            Key key = keys.get(session.prefix());
            byte[] data = session.transaction().get(key.cardinality()).join();
            return ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN).getInt();
        } catch (ExecutionException e) {
            throw new KronotopException(e);
        }
    }

    /**
     * Increments the number of used bytes associated with the given session by the specified length.
     *
     * @param session The session object containing the current transaction and prefix information.
     * @param length  The amount of bytes to add to the used bytes count.
     */
    void increaseUsedBytes(VolumeSession session, long length) {
        byte[] delta = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong(length).array();
        try {
            Key key = keys.get(session.prefix());
            session.transaction().mutate(MutationType.ADD, key.usedBytes(), delta);
        } catch (ExecutionException e) {
            throw new KronotopException(e);
        }
    }

    /**
     * Resets the used bytes count associated with the given session by clearing it in the transaction.
     *
     * @param session The session object containing the current transaction and prefix information.
     * @throws KronotopException if an execution exception occurs while performing the operation.
     */
    void resetUsedBytes(VolumeSession session) {
        try {
            Key key = keys.get(session.prefix());
            session.transaction().clear(key.usedBytes());
        } catch (ExecutionException e) {
            throw new KronotopException(e);
        }
    }

    /**
     * Retrieves the number of used bytes associated with the given session.
     *
     * @param session The session object containing the current transaction and prefix information.
     * @return The number of used bytes associated with the session.
     * @throws KronotopException if an execution exception occurs while retrieving the used bytes data.
     */
    long usedBytes(VolumeSession session) {
        try {
            Key key = keys.get(session.prefix());
            byte[] data = session.transaction().get(key.usedBytes()).join();
            return ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN).getLong();
        } catch (ExecutionException e) {
            throw new KronotopException(e);
        }
    }

    /**
     * A cache loader that is responsible for loading keys from a given subspace and segment within that subspace.
     * The keys represent specific metadata such as cardinality and used bytes associated with a given prefix.
     */
    private static class KeysLoader extends CacheLoader<Prefix, Key> {
        private final VolumeSubspace subspace;
        private final String segmentName;

        private KeysLoader(VolumeSubspace subspace, String segmentName) {
            this.subspace = subspace;
            this.segmentName = segmentName;
        }

        @Override
        public @Nonnull Key load(@Nonnull Prefix prefix) {
            byte[] cardinality = subspace.packSegmentCardinalityKey(segmentName, prefix);
            byte[] usedBytes = subspace.packSegmentUsedBytesKey(segmentName, prefix);
            return new Key(cardinality, usedBytes);
        }
    }

    /**
     * The Key record is used to store metadata for a specific segment within a subspace.
     * It encapsulates two byte arrays, one representing the cardinality and the other representing the used bytes.
     *
     * @param cardinality Byte array representing the cardinality of the segment.
     * @param usedBytes   Byte array representing the number of used bytes for the segment.
     */
    private record Key(byte[] cardinality, byte[] usedBytes) {
    }
}
