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

package com.kronotop.protocol;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulConnection;

import java.util.List;

public interface KronotopAsyncCommands<K, V> {
    RedisFuture<String> auth(K key, V value);

    RedisFuture<String> begin();

    RedisFuture<String> rollback();

    RedisFuture<String> commit();

    RedisFuture<Long> commitAndGetCommittedVersion();

    RedisFuture<V> commitAndGetVersionstamp();

    RedisFuture<List<Object>> namespaceList(K namespace);

    RedisFuture<List<Object>> namespaceListOpen(K namespace);

    RedisFuture<Long> namespaceExists(K namespace);

    RedisFuture<String> namespaceOpen(K namespace);

    RedisFuture<String> namespaceRemove(K namespace);

    RedisFuture<String> namespaceMove(K oldNamespace, K newNamespace);

    RedisFuture<String> namespaceCreate(K namespace);

    RedisFuture<String> namespaceCreate(K namespace, NamespaceArgs args);

    RedisFuture<String> namespaceCreateOrOpen(K namespace);

    RedisFuture<String> namespaceCreateOrOpen(K namespace, NamespaceArgs args);

    RedisFuture<String> zput(String namespace, K key, V value);

    RedisFuture<V> zget(String namespace, K key);

    RedisFuture<String> zdel(String namespace, K key);

    RedisFuture<String> zdelprefix(byte[] key);

    RedisFuture<String> zdelrange(String namespace, ZDelRangeArgs args);

    RedisFuture<List<Object>> zgetrange(String namespace, ZGetRangeArgs args);

    RedisFuture<V> zgetkey(String namespace, ZGetKeyArgs args);

    RedisFuture<String> snapshotRead(SnapshotReadArgs args);

    RedisFuture<String> zmutate(String namespace, K key, V param, ZMutateArgs args);

    RedisFuture<Long> zgetrangesize(String namespace, ZGetRangeSizeArgs args);

    RedisFuture<Long> getapproximatesize();

    RedisFuture<Long> getreadversion();

    StatefulConnection<K, V> getUnderlyingConnection();
}
