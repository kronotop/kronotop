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

package com.kronotop.protocol;

import java.util.List;

public interface KronotopCommands<K, V> {
    String auth(K key, V value);

    String begin();

    String rollback();

    String commit();

    List<Object> commit(CommitArgs args);

    Long commitReturningCommittedVersion();

    V commitReturningVersionstamp();

    String namespaceCreate(K namespace);

    String namespaceCreate(K namespace, NamespaceArgs args);

    List<String> namespaceList(K namespace);

    List<String> namespaceMove(K oldNamespace, K newNamespace);

    String namespaceRemove(K namespace);

    String namespaceUse(K namespace);

    Long namespaceExists(K namespace);

    String namespaceCurrent();

    String zset(K key, V value);

    V zget(K key);

    String zdel(K key);

    String zdelprefix(byte[] key);

    String zdelprefix(String namespace, ZDelRangeArgs args);

    List<Object> zgetrange(ZGetRangeArgs args);

    V zgetkey(String namespace, ZGetKeyArgs args);

    String snapshotRead(SnapshotReadArgs args);

    String zmutate(K key, V param, ZMutateArgs args);

    Long zgetRangeSize(String namespace, ZGetRangeSizeArgs args);

    Long getApproximateSize();

    Long getReadVersion();
}
