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

    RedisFuture<List<Object>> commit(CommitArgs args);

    RedisFuture<List<Object>> namespaceList(K namespace);

    RedisFuture<Long> namespaceExists(K namespace);

    RedisFuture<String> namespaceRemove(K namespace);

    RedisFuture<String> namespaceMove(K oldNamespace, K newNamespace);

    RedisFuture<String> namespaceCreate(K namespace);

    RedisFuture<String> namespaceUse(K namespace);

    RedisFuture<String> namespaceCurrent();

    RedisFuture<String> namespaceCreate(K namespace, NamespaceArgs args);

    RedisFuture<String> zset(K key, V value);

    RedisFuture<V> zget(K key);

    RedisFuture<String> zdel(K key);

    RedisFuture<String> zdelprefix(byte[] key);

    RedisFuture<String> zdelrange(ZDelRangeArgs args);

    RedisFuture<List<Object>> zgetrange(ZGetRangeArgs args);

    RedisFuture<V> zgetkey(ZGetKeyArgs args);

    RedisFuture<String> snapshotRead(SnapshotReadArgs args);

    RedisFuture<String> zmutate(K key, V param, ZMutateArgs args);

    RedisFuture<Long> zgetRangeSize(ZGetRangeSizeArgs args);

    RedisFuture<Long> getApproximateSize();

    RedisFuture<Long> getReadVersion();

    StatefulConnection<K, V> getUnderlyingConnection();
}
