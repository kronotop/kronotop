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

package com.kronotop.watcher;

import com.google.common.util.concurrent.Striped;
import com.kronotop.Context;
import com.kronotop.KronotopService;
import com.kronotop.server.ChannelAttributes;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelId;
import io.netty.util.Attribute;

import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;

/**
 * The Watcher class is an implementation of the KronotopService interface.
 * It provides methods to watch, unwatch, and check the modification status of keys.
 */
public class Watcher implements KronotopService {
    public static final String NAME = "Watcher";
    private final ConcurrentMap<String, WatchedKey> watchedKeys = new ConcurrentHashMap<>();
    private final Striped<Lock> striped = Striped.lazyWeakLock(271);

    /**
     * Watches a key for changes and associates a given channelId with it.
     *
     * @param channelId The channelId to associate with the key
     * @param key       The key to watch
     * @return The version of the watched key
     */
    public Long watchKey(ChannelId channelId, String key) {
        AtomicLong version = new AtomicLong();
        watchedKeys.compute(key, (k, watchedKey) -> {
            if (watchedKey == null) {
                watchedKey = new WatchedKey();
            }
            version.set(watchedKey.getVersion());
            watchedKey.getChannels().add(channelId);
            return watchedKey;
        });
        return version.get();
    }

    /**
     * Removes the association between a channelId and a watched key.
     *
     * @param channelId The channelId associated with the key
     * @param key       The key to unwatch
     */
    public void unwatchKey(ChannelId channelId, String key) {
        watchedKeys.compute(key, (k, watchedKey) -> {
            if (watchedKey == null) {
                return null;
            }
            Set<ChannelId> channels = watchedKey.getChannels();
            channels.remove(channelId);
            if (channels.isEmpty()) {
                return null;
            }
            return watchedKey;
        });
    }

    /**
     * Increases the version of a watched key.
     *
     * @param key The key for which to increase the version
     */
    public void increaseWatchedKeyVersion(String key) {
        watchedKeys.compute(key, (k, watchedKey) -> {
            if (watchedKey == null) {
                // no watcher on this key
                return null;
            }
            watchedKey.increaseVersion();
            return watchedKey;
        });
    }

    /**
     * Checks if a watched key has been modified since a given version.
     *
     * @param key     The key to check
     * @param version The version to compare against
     * @return True if the key has been modified, false otherwise
     */
    public Boolean isModified(String key, Long version) {
        AtomicBoolean result = new AtomicBoolean();
        watchedKeys.compute(key, (k, watchedKey) -> {
            if (watchedKey == null) {
                return null;
            }
            Long currentVersion = watchedKey.getVersion();
            if (!currentVersion.equals(version)) {
                result.set(true);
            }
            return watchedKey;
        });

        return result.get();
    }

    /**
     * Checks whether there are any keys being watched by the Watcher object.
     *
     * @return true if there are watched keys, false otherwise
     */
    public Boolean hasWatchers() {
        return !watchedKeys.isEmpty();
    }

    /**
     * Cleans up the {@link ChannelHandlerContext} by unwatching all keys associated with the channel.
     * Removes the association between the channel and watched keys, and releases the lock.
     *
     * @param ctx the ChannelHandlerContext to cleanup
     */
    public void cleanupChannelHandlerContext(ChannelHandlerContext ctx) {
        Lock lock = striped.get(ctx.channel().id());
        lock.lock();
        try {
            Attribute<HashMap<String, Long>> watchedKeysAttr = ctx.channel().attr(ChannelAttributes.WATCHED_KEYS);
            HashMap<String, Long> watchedKeys = watchedKeysAttr.get();
            if (watchedKeys == null) {
                return;
            }

            for (String key : watchedKeys.keySet()) {
                this.unwatchKey(ctx.channel().id(), key);
            }
            watchedKeysAttr.set(null);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public Context getContext() {
        return null;
    }

    @Override
    public void shutdown() {
    }
}
