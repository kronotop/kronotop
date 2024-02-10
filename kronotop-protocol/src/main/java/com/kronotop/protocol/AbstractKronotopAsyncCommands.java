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
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.protocol.AsyncCommand;
import io.lettuce.core.protocol.RedisCommand;

import java.util.List;

public abstract class AbstractKronotopAsyncCommands<K, V> implements KronotopAsyncCommands<K, V> {
    private final KronotopCommandBuilder<K, V> commandBuilder;
    private final StatefulConnection<K, V> connection;

    protected AbstractKronotopAsyncCommands(StatefulConnection<K, V> connection, RedisCodec<K, V> codec) {
        this.commandBuilder = new KronotopCommandBuilder<>(codec);
        this.connection = connection;
    }

    @Override
    public RedisFuture<String> auth(K key, V value) {
        return dispatch(commandBuilder.auth(key, value));
    }

    @Override
    public RedisFuture<String> begin() {
        return dispatch(commandBuilder.begin());
    }

    @Override
    public RedisFuture<String> rollback() {
        return dispatch(commandBuilder.rollback());
    }

    @Override
    public RedisFuture<String> commit() {
        return dispatch(commandBuilder.commit());
    }

    @Override
    public RedisFuture<Long> commitAndGetCommittedVersion() {
        return dispatch(commandBuilder.commitAndGetCommittedVersion());
    }

    @Override
    public RedisFuture<V> commitAndGetVersionstamp() {
        return dispatch(commandBuilder.commitAndGetVersionstamp());
    }

    @Override
    public RedisFuture<String> namespaceCreate(K namespace) {
        return dispatch(commandBuilder.namespaceCreate(namespace, null));
    }

    @Override
    public RedisFuture<String> namespaceCreate(K namespace, NamespaceArgs args) {
        return dispatch(commandBuilder.namespaceCreate(namespace, args));
    }

    @Override
    public RedisFuture<String> namespaceUse(K namespace) {
        return dispatch(commandBuilder.namespaceUse(namespace));
    }

    @Override
    public RedisFuture<List<Object>> namespaceList(K namespace) {
        return dispatch(commandBuilder.namespaceList(namespace));
    }

    @Override
    public RedisFuture<String> namespaceRemove(K namespace) {
        return dispatch(commandBuilder.namespaceRemove(namespace));
    }

    @Override
    public RedisFuture<String> namespaceMove(K oldNamespace, K newNamespace) {
        return dispatch(commandBuilder.namespaceMove(oldNamespace, newNamespace));
    }

    @Override
    public RedisFuture<Long> namespaceExists(K namespace) {
        return dispatch(commandBuilder.namespaceExists(namespace));
    }

    @Override
    public RedisFuture<String> namespaceCurrent() {
        return dispatch(commandBuilder.namespaceCurrent());
    }

    @Override
    public RedisFuture<String> zset(K key, V value) {
        return dispatch(commandBuilder.zset(key, value));
    }

    @Override
    public RedisFuture<V> zget(K key) {
        return dispatch(commandBuilder.zget(key));
    }

    @Override
    public RedisFuture<String> zdel(K key) {
        return dispatch(commandBuilder.zdel(key));
    }

    @Override
    public RedisFuture<String> zdelprefix(byte[] key) {
        return dispatch(commandBuilder.zdelprefix(key));
    }

    @Override
    public RedisFuture<String> zdelrange(ZDelRangeArgs args) {
        return dispatch(commandBuilder.zdelrange(args));
    }

    @Override
    public RedisFuture<List<Object>> zgetrange(ZGetRangeArgs args) {
        return dispatch(commandBuilder.zgetrange(args));
    }

    @Override
    public RedisFuture<V> zgetkey(ZGetKeyArgs args) {
        return dispatch(commandBuilder.zgetkey(args));
    }

    @Override
    public RedisFuture<String> snapshotRead(SnapshotReadArgs args) {
        return dispatch(commandBuilder.snapshotRead(args));
    }

    @Override
    public RedisFuture<String> zmutate(K key, V param, ZMutateArgs args) {
        return dispatch(commandBuilder.zmutate(key, param, args));
    }

    @Override
    public RedisFuture<Long> zgetrangesize(ZGetRangeSizeArgs args) {
        return dispatch(commandBuilder.zgetrangesize(args));
    }

    @Override
    public RedisFuture<Long> getapproximatesize() {
        return dispatch(commandBuilder.getapproximatesize());
    }

    @Override
    public RedisFuture<Long> getreadversion() {
        return dispatch(commandBuilder.getreadversion());
    }

    private <T> AsyncCommand<K, V, T> dispatch(RedisCommand<K, V, T> cmd) {
        AsyncCommand<K, V, T> asyncCommand = new AsyncCommand<>(cmd);
        RedisCommand<K, V, T> dispatched = connection.dispatch(asyncCommand);
        if (dispatched instanceof AsyncCommand) {
            return (AsyncCommand<K, V, T>) dispatched;
        }
        return asyncCommand;
    }

    @Override
    public StatefulConnection<K, V> getUnderlyingConnection() {
        return connection;
    }

    @Override
    public RedisFuture<Object> sql(String query) {
        return dispatch(commandBuilder.sql(query));
    }

    @Override
    public RedisFuture<String> sqlSetSchema(String schema) {
        return dispatch(commandBuilder.sqlSetSchema(schema));
    }

    @Override
    public RedisFuture<List<Object>> sqlGetSchema() {
        return dispatch(commandBuilder.sqlGetSchema());
    }
}
