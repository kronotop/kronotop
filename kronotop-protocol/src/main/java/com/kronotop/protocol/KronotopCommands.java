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

import java.util.List;

public interface KronotopCommands<K, V> {
    String auth(K key, V value);

    String begin();

    String rollback();

    String commit();

    Long commitAndGetCommittedVersion();

    V commitAndGetVersionstamp();

    String namespaceCreate(K namespace);

    String namespaceCreate(K namespace, NamespaceArgs args);

    String namespaceCreateOrOpen(K namespace);

    String namespaceCreateOrOpen(K namespace, NamespaceArgs args);

    List<String> namespaceList(K namespace);

    List<String> namespaceMove(K oldNamespace, K newNamespace);

    List<String> namespaceListOpen(K namespace);

    String namespaceOpen(K namespace);

    String namespaceRemove(K namespace);

    Long namespaceExists(K namespace);

    String zput(String namespace, K key, V value);

    V zget(String namespace, K key);

    String zdel(String namespace, K key);

    String zdelprefix(byte[] key);

    String zdelprefix(String namespace, ZDelRangeArgs args);

    List<Object> zgetrange(String namespace, ZGetRangeArgs args);

    V zgetkey(String namespace, ZGetKeyArgs args);

    String snapshotRead(SnapshotReadArgs args);

    String zmutate(String namespace, K key, V param, ZMutateArgs args);

    Long zgetrangesize(String namespace, ZGetRangeSizeArgs args);

    Long getapproximatesize();

    Long getreadversion();
}
