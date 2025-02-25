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

package com.kronotop.watcher;

import com.google.common.util.concurrent.Striped;
import com.kronotop.Context;
import com.kronotop.KronotopService;
import com.kronotop.server.Session;
import com.kronotop.server.SessionAttributes;
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
     * Watches the specified key for the given session and returns the current version of the key.
     * If the key is already being watched, the session is added to the existing watchers.
     *
     * @param sessionId The ID of the session that is watching the key.
     * @param key       The key to be watched by the session.
     * @return The current version of the key being watched.
     */
    public Long watchKey(Long sessionId, String key) {
        AtomicLong version = new AtomicLong();
        watchedKeys.compute(key, (k, watchedKey) -> {
            if (watchedKey == null) {
                watchedKey = new WatchedKey();
            }
            version.set(watchedKey.getVersion());
            watchedKey.getSessionIds().add(sessionId);
            return watchedKey;
        });
        return version.get();
    }

    /**
     * Removes the specified session from the watchers of a given key. If no sessions remain
     * watching the key, the key is removed from the watched keys map.
     *
     * @param sessionId The ID of the session that should be removed from watching the key.
     * @param key       The key for which the session should be unwatched.
     */
    public void unwatchKey(Long sessionId, String key) {
        watchedKeys.compute(key, (k, watchedKey) -> {
            if (watchedKey == null) {
                return null;
            }
            Set<Long> sessionIds = watchedKey.getSessionIds();
            sessionIds.remove(sessionId);
            if (sessionIds.isEmpty()) {
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
     * Unwatches all the keys currently being watched by the given session.
     * This method retrieves the keys watched by the session, removes the session
     * as a watcher for each key, and clears the session's watched keys.
     * If no keys are being watched, the method performs no operation.
     *
     * @param session The session whose watched keys are to be unwatched. It is expected
     *                that the session's watched keys are stored as an attribute, which will
     *                be accessed and cleared by this method.
     */
    public void unwatchWatchedKeys(Session session) {
        Lock lock = striped.get(session.getClientId());
        lock.lock();
        try {
            Attribute<HashMap<String, Long>> watchedKeysAttr = session.attr(SessionAttributes.WATCHED_KEYS);
            HashMap<String, Long> watchedKeys = watchedKeysAttr.get();
            if (watchedKeys == null) {
                return;
            }

            for (String key : watchedKeys.keySet()) {
                unwatchKey(session.getClientId(), key);
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
