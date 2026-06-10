/*
 * Copyright (c) 2023-2026 Burak Sezer
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */


package com.kronotop.commands;

import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.output.ArrayOutput;
import io.lettuce.core.output.IntegerOutput;
import io.lettuce.core.output.StatusOutput;
import io.lettuce.core.protocol.Command;
import io.lettuce.core.protocol.CommandArgs;

import java.util.List;

/**
 * Provides methods for constructing commands for core operations: authentication,
 * transactions, namespaces, session management, and FDB utilities.
 *
 * @param <K> the type of keys handled by this command builder
 * @param <V> the type of values handled by this command builder
 */
public class KronotopCommandBuilder<K, V> extends BaseKronotopCommandBuilder<K, V> {

    public KronotopCommandBuilder(RedisCodec<K, V> codec) {
        super(codec);
    }

    /**
     * Constructs and executes an AUTH command with the specified key and value.
     *
     * @param key   the key associated with the authentication command; must not be null
     * @param value the value associated with the authentication command; must not be null
     * @return a {@link Command} instance representing the executed AUTH operation, containing the result as a string response from the server
     */
    public Command<K, V, String> auth(K key, V value) {
        return createCommand(CommandType.AUTH, new StatusOutput<>(codec), key, value);
    }

    /**
     * Constructs and executes a BEGIN command.
     * This command is typically used to start a transactional operation.
     *
     * @return a {@link Command} instance representing the executed BEGIN operation,
     * containing the result as a string response from the server.
     */
    public Command<K, V, String> begin() {
        return createCommand(CommandType.BEGIN, new StatusOutput<>(codec));
    }

    /**
     * Constructs and executes a ROLLBACK command.
     * This command is typically used to revert all modifications
     * made during the active transactional operation.
     *
     * @return a {@link Command} instance representing the executed ROLLBACK operation,
     * containing the result as a string response from the server.
     */
    public Command<K, V, String> rollback() {
        return createCommand(CommandType.ROLLBACK, new StatusOutput<>(codec));
    }

    /**
     * Constructs and executes a COMMIT command.
     * This command is typically used to commit all modifications made during
     * the active transactional operation.
     *
     * @return a {@link Command} instance representing the executed COMMIT operation,
     * containing the result as a string response from the server.
     */
    public Command<K, V, String> commit() {
        return createCommand(CommandType.COMMIT, new StatusOutput<>(codec));
    }

    public Command<K, V, List<Object>> commit(CommitArgs commitArgs) {
        CommandArgs<K, V> args = new CommandArgs<>(codec);
        commitArgs.build(args);
        return createCommand(CommandType.COMMIT, new ArrayOutput<>(codec), args);
    }

    /**
     * Constructs and executes a NAMESPACE REMOVE command with the specified namespace.
     * This command is used to remove an existing namespace.
     *
     * @param namespace the namespace to be removed; must not be null.
     * @return a {@link Command} instance representing the executed NAMESPACE REMOVE operation,
     * containing the result as a string response from the server.
     */
    public Command<K, V, String> namespaceRemove(K namespace) {
        CommandArgs<K, V> args = new CommandArgs<>(codec).
                add(NamespaceKeywords.REMOVE).
                addKey(namespace);
        return createCommand(CommandType.NAMESPACE, new StatusOutput<>(codec), args);
    }

    public Command<K, V, String> namespacePurge(K namespace) {
        CommandArgs<K, V> args = new CommandArgs<>(codec).
                add(NamespaceKeywords.PURGE).
                addKey(namespace);
        return createCommand(CommandType.NAMESPACE, new StatusOutput<>(codec), args);
    }

    /**
     * Constructs and executes a NAMESPACE USE command with the specified namespace.
     * This command is used to set the context to the specified namespace.
     *
     * @param namespace the namespace to be used; must not be null.
     * @return a {@link Command} instance representing the executed NAMESPACE USE operation,
     * containing the result as a string response from the server.
     */
    public Command<K, V, String> namespaceUse(K namespace) {
        CommandArgs<K, V> args = new CommandArgs<>(codec).
                add(NamespaceKeywords.USE).
                addKey(namespace);
        return createCommand(CommandType.NAMESPACE, new StatusOutput<>(codec), args);
    }

    /**
     * Constructs and executes a NAMESPACE EXISTS command with the specified namespace.
     * This command is used to check whether a namespace exists.
     *
     * @param namespace the namespace to check for existence; must not be null.
     * @return a {@link Command} instance representing the executed NAMESPACE EXISTS operation,
     * containing the result as a {@link Long} indicating the existence of the namespace (1 if exists, 0 if not).
     */
    public Command<K, V, Long> namespaceExists(K namespace) {
        CommandArgs<K, V> args = new CommandArgs<>(codec).
                add(NamespaceKeywords.EXISTS).
                addKey(namespace);
        return createCommand(CommandType.NAMESPACE, new IntegerOutput<>(codec), args);
    }

    /**
     * Constructs and executes a NAMESPACE LIST command with the optional specified namespace.
     * This command is used to retrieve a list of namespaces or information about a specific namespace
     * if the namespace parameter is provided.
     *
     * @param namespace the specific namespace to list information for; may be null to list all namespaces.
     * @return a {@link Command} instance representing the executed NAMESPACE LIST operation,
     * containing the result as a list of objects from the server.
     */
    public Command<K, V, List<Object>> namespaceList(K namespace) {
        CommandArgs<K, V> args = new CommandArgs<>(codec).
                add(NamespaceKeywords.LIST);
        if (namespace != null) {
            args.addKey(namespace);
        }
        return createCommand(CommandType.NAMESPACE, new ArrayOutput<>(codec), args);
    }

    /**
     * Constructs and executes a NAMESPACE CREATE command with the specified namespace.
     * This command is used to create a new namespace.
     *
     * @param namespace the name of the namespace to be created; must not be null.
     * @return a {@link Command} instance representing the executed NAMESPACE CREATE operation,
     * containing the result as a string response from the server.
     */
    public Command<K, V, String> namespaceCreate(K namespace) {
        CommandArgs<K, V> args = new CommandArgs<>(codec).
                add(NamespaceKeywords.CREATE).
                addKey(namespace);
        return createCommand(CommandType.NAMESPACE, new StatusOutput<>(codec), args);
    }

    /**
     * Constructs and executes a NAMESPACE MOVE command with the specified source and target namespaces.
     * This command is used to move data from one namespace to another.
     *
     * @param oldNamespace the source namespace from which data will be moved; must not be null.
     * @param newNamespace the target namespace to which data will be moved; must not be null.
     * @return a {@link Command} instance representing the executed NAMESPACE MOVE operation,
     * containing the result as a string response from the server.
     */
    public Command<K, V, String> namespaceMove(K oldNamespace, K newNamespace) {
        CommandArgs<K, V> args = new CommandArgs<>(codec).
                add(NamespaceKeywords.MOVE).
                addKeys(oldNamespace, newNamespace);
        return createCommand(CommandType.NAMESPACE, new StatusOutput<>(codec), args);
    }

    /**
     * Constructs and executes a NAMESPACE CURRENT command.
     * This command is used to retrieve the current active namespace context.
     *
     * @return a {@link Command} instance representing the executed NAMESPACE CURRENT operation,
     * containing the result as a string response from the server.
     */
    public Command<K, V, String> namespaceCurrent() {
        CommandArgs<K, V> args = new CommandArgs<>(codec).
                add(NamespaceKeywords.CURRENT);
        return createCommand(CommandType.NAMESPACE, new StatusOutput<>(codec), args);
    }

    /**
     * Executes a snapshot read command using the specified arguments.
     *
     * @param snapshotReadArgs the arguments for the snapshot read command; can be null if no arguments are provided
     * @return a command representing the snapshot read operation
     */
    public Command<K, V, String> snapshotRead(SnapshotReadArgs snapshotReadArgs) {
        CommandArgs<K, V> args = new CommandArgs<>(codec);
        if (snapshotReadArgs != null) {
            snapshotReadArgs.build(args);
        }
        return createCommand(CommandType.SNAPSHOTREAD, new StatusOutput<>(codec), args);
    }

    /**
     * Retrieves a command to estimate the approximate size of a data structure.
     *
     * @return a command of type {@code Command<K, V, Long>} to fetch the approximate size.
     */
    public Command<K, V, Long> getApproximateSize() {
        return createCommand(CommandType.GETAPPROXIMATESIZE, new IntegerOutput<>(codec));
    }

    /**
     * Creates and returns a command to retrieve the current read version of the database.
     *
     * @return a Command object that fetches the read version in Long format
     */
    public Command<K, V, Long> getreadversion() {
        return createCommand(CommandType.GETREADVERSION, new IntegerOutput<>(codec));
    }

    /**
     * Creates and returns a command to retrieve a fresh or cached read version of the database.
     *
     * @param tickArgs the arguments selecting the read version mode, FRESH or CACHED
     * @return a Command object that fetches the read version in Long format
     */
    public Command<K, V, Long> tick(TickArgs tickArgs) {
        CommandArgs<K, V> args = new CommandArgs<>(codec);
        tickArgs.build(args);
        return createCommand(CommandType.TICK, new IntegerOutput<>(codec), args);
    }

    /**
     * Lists all session attributes and their current values.
     *
     * @return a {@link Command} instance representing the SESSION.ATTRIBUTE LIST operation,
     * containing a list of attribute name-value pairs
     */
    public Command<K, V, List<Object>> sessionAttributeList() {
        CommandArgs<K, V> args = new CommandArgs<>(codec).
                add(SessionAttributeKeywords.LIST);
        return createCommand(CommandType.SESSION_ATTRIBUTE, new ArrayOutput<>(codec), args);
    }

    /**
     * Sets a session attribute to the specified value.
     *
     * @param attribute the attribute to set (e.g., REPLY_TYPE, INPUT_TYPE, LIMIT, OBJECT_ID_FORMAT)
     * @param value     the value to set for the attribute
     * @return a {@link Command} instance representing the SESSION.ATTRIBUTE SET operation
     */
    public Command<K, V, String> sessionAttributeSet(SessionAttributeKeywords attribute, K value) {
        CommandArgs<K, V> args = new CommandArgs<>(codec).
                add(SessionAttributeKeywords.SET).
                add(attribute).
                addKey(value);
        return createCommand(CommandType.SESSION_ATTRIBUTE, new StatusOutput<>(codec), args);
    }

    /**
     * Closes the session and resets all session state while keeping the connection open.
     *
     * @return a {@link Command} instance representing the SESSION.CLOSE operation
     */
    public Command<K, V, String> sessionClose() {
        return createCommand(CommandType.SESSION_CLOSE, new StatusOutput<>(codec));
    }
}
