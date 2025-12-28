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


package com.kronotop.protocol;

import com.kronotop.protocol.zmap.*;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.output.ArrayOutput;
import io.lettuce.core.output.DoubleOutput;
import io.lettuce.core.output.IntegerOutput;
import io.lettuce.core.output.StatusOutput;
import io.lettuce.core.output.ValueOutput;
import io.lettuce.core.protocol.Command;
import io.lettuce.core.protocol.CommandArgs;

import java.util.List;

/**
 * The {@code KronotopCommandBuilder} class provides methods for constructing and executing commands
 * for managing namespaces, transactional operations, and working with sorted sets in a Redis-like
 * database system. This class supports a variety of commands, including authentication, namespace
 * operations, transaction management, and sorted set manipulation.
 * <p>
 * This builder operates using a specified {@link RedisCodec} for serialization and deserialization
 * of keys and values.
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
     * Constructs and executes a ZSET command with the specified key and value.
     * This command is typically used to set a value for a given key in a sorted set.
     *
     * @param key   the key associated with the sorted set; must not be null.
     * @param value the value to be set for the key; must not be null.
     * @return a {@link Command} instance representing the executed ZSET operation,
     * containing the result as a string response from the server.
     */
    public Command<K, V, String> zset(K key, V value) {
        CommandArgs<K, V> args = new CommandArgs<>(codec).
                addKey(key).
                addValue(value);
        return createCommand(CommandType.ZSET, new StatusOutput<>(codec), args);
    }

    /**
     * Constructs and executes a ZGET command with the specified key.
     * This command is used to retrieve a value associated with the given key
     * in a sorted set.
     *
     * @param key the key whose associated value should be retrieved; must not be null.
     * @return a {@link Command} instance representing the executed ZGET operation,
     * containing the retrieved value associated with the specified key.
     */
    public Command<K, V, V> zget(K key) {
        CommandArgs<K, V> args = new CommandArgs<>(codec).
                addKey(key);
        return createCommand(CommandType.ZGET, new ValueOutput<>(codec), args);
    }

    /**
     * Constructs and executes a ZDEL command with the specified key.
     * This command is used to delete a key from a sorted set.
     *
     * @param key the key to be deleted; must not be null.
     * @return a {@link Command} instance representing the executed ZDEL operation,
     * containing the result as a string response from the server.
     */
    public Command<K, V, String> zdel(K key) {
        CommandArgs<K, V> args = new CommandArgs<>(codec).
                addKey(key);
        return createCommand(CommandType.ZDEL, new StatusOutput<>(codec), args);
    }

    /**
     * Constructs and executes a ZDELRANGE command with the specified arguments.
     *
     * @param zDelRangeArgs the arguments specifying the range to delete; must not be null and should
     *                      specify a valid range for the operation
     * @return a {@link Command} instance representing the executed ZDELRANGE operation, containing
     * the result as a string response from the server
     */
    public Command<K, V, String> zdelrange(ZDelRangeArgs zDelRangeArgs) {
        CommandArgs<K, V> args = new CommandArgs<>(codec);
        if (zDelRangeArgs != null) {
            zDelRangeArgs.build(args);
        }
        return createCommand(CommandType.ZDELRANGE, new StatusOutput<>(codec), args);
    }

    /**
     * Retrieves a range of elements from a sorted set based on the provided {@link ZGetRangeArgs}.
     *
     * @param zGetRangeArgs the arguments specifying the range and other options for the operation; can be null to use default behavior
     * @return a {@link Command} that returns a {@link List} of {@link Object} representing the range of elements in the sorted set
     */
    public Command<K, V, List<Object>> zgetrange(ZGetRangeArgs zGetRangeArgs) {
        CommandArgs<K, V> args = new CommandArgs<>(codec);
        if (zGetRangeArgs != null) {
            zGetRangeArgs.build(args);
        }
        return createCommand(CommandType.ZGETRANGE, new ArrayOutput<>(codec), args);
    }

    /**
     * Executes a command to retrieve a key from a sorted set or a similar data structure,
     * based on the provided {@code ZGetKeyArgs}.
     *
     * @param zGetKeyArgs the arguments specifying the parameters for the command, such as key and additional options.
     *                    If {@code null}, no additional arguments are considered.
     * @return a {@link Command} object representing the operation to retrieve a key, including
     * the configuration and expected output type.
     */
    public Command<K, V, V> zgetkey(ZGetKeyArgs zGetKeyArgs) {
        CommandArgs<K, V> args = new CommandArgs<>(codec);
        if (zGetKeyArgs != null) {
            zGetKeyArgs.build(args);
        }
        return createCommand(CommandType.ZGETKEY, new ValueOutput<>(codec), args);
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
     * Executes the ZMUTATE command with the specified key, parameter, and mutation arguments.
     *
     * @param key         the key for the sorted set on which the mutation operation will be performed
     * @param param       the value or parameter to be applied for the mutation operation
     * @param zMutateArgs the additional arguments for the mutation operation, allowing fine-tuned control of the behavior
     * @return a {@code Command} object representing the ZMUTATE operation
     */
    public Command<K, V, String> zmutate(K key, V param, ZMutateArgs zMutateArgs) {
        CommandArgs<K, V> args = new CommandArgs<>(codec).
                addKey(key).
                addValue(param);
        if (zMutateArgs != null) {
            zMutateArgs.build(args);
        }
        return createCommand(CommandType.ZMUTATE, new StatusOutput<>(codec), args);
    }

    /**
     * Executes a ZGETRANGESIZE command to calculate the size of ranges from sorted sets based on the provided arguments.
     *
     * @param zGetRangeSizeArgs the arguments defining the range dimensions and conditions for the ZGETRANGESIZE command; can be null.
     * @return the command object that, when executed, will return the calculated size of the specified range as a Long value.
     */
    public Command<K, V, Long> zgetrangesize(ZGetRangeSizeArgs zGetRangeSizeArgs) {
        CommandArgs<K, V> args = new CommandArgs<>(codec);
        if (zGetRangeSizeArgs != null) {
            zGetRangeSizeArgs.build(args);
        }
        return createCommand(CommandType.ZGETRANGESIZE, new IntegerOutput<>(codec), args);
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
     * Atomically increments a 64-bit integer value stored at the specified key.
     * If the key does not exist, it is created with the increment value.
     * Uses FoundationDB's atomic ADD mutation for lock-free concurrent increments.
     *
     * @param key   the key whose value should be incremented; must not be null
     * @param value the increment value (can be negative for decrement)
     * @return a {@link Command} instance representing the ZINC.I64 operation,
     * containing OK as the response
     */
    public Command<K, V, String> zinci64(K key, long value) {
        CommandArgs<K, V> args = new CommandArgs<>(codec).
                addKey(key).
                add(value);
        return createCommand(CommandType.ZINC_I64, new StatusOutput<>(codec), args);
    }

    /**
     * Retrieves a 64-bit integer value stored at the specified key.
     * Returns the value as a long integer, or NULL if the key does not exist.
     *
     * @param key the key whose value should be retrieved; must not be null
     * @return a {@link Command} instance representing the ZGET.I64 operation,
     * containing the value as a Long, or null if the key doesn't exist
     */
    public Command<K, V, Long> zgeti64(K key) {
        CommandArgs<K, V> args = new CommandArgs<>(codec).
                addKey(key);
        return createCommand(CommandType.ZGET_I64, new IntegerOutput<>(codec), args);
    }

    /**
     * Increments a 64-bit floating point value stored at the specified key.
     * If the key does not exist, it is created with the increment value.
     * Validates that both delta and result are finite IEEE-754 doubles.
     *
     * @param key   the key whose value should be incremented; must not be null
     * @param value the increment value (can be negative for decrement)
     * @return a {@link Command} instance representing the ZINC.F64 operation,
     * containing OK as the response
     */
    public Command<K, V, String> zincf64(K key, double value) {
        CommandArgs<K, V> args = new CommandArgs<>(codec).
                addKey(key).
                add(value);
        return createCommand(CommandType.ZINC_F64, new StatusOutput<>(codec), args);
    }

    /**
     * Retrieves a 64-bit floating point value stored at the specified key.
     * Returns the value as a double, or NULL if the key does not exist.
     *
     * @param key the key whose value should be retrieved; must not be null
     * @return a {@link Command} instance representing the ZGET.F64 operation,
     * containing the value as a Double, or null if the key doesn't exist
     */
    public Command<K, V, Double> zgetf64(K key) {
        CommandArgs<K, V> args = new CommandArgs<>(codec).
                addKey(key);
        return createCommand(CommandType.ZGET_F64, new DoubleOutput<>(codec), args);
    }

    /**
     * Increments a 128-bit decimal value stored at the specified key.
     * If the key does not exist, it is created with the increment value.
     * Uses IEEE-754 Decimal128 (BID encoding) for arbitrary precision decimals.
     *
     * @param key   the key whose value should be incremented; must not be null
     * @param value the increment value as a decimal string (can be negative for decrement)
     * @return a {@link Command} instance representing the ZINC.D128 operation,
     * containing OK as the response
     */
    public Command<K, V, String> zincd128(K key, String value) {
        CommandArgs<K, V> args = new CommandArgs<>(codec).
                addKey(key).
                add(value);
        return createCommand(CommandType.ZINC_D128, new StatusOutput<>(codec), args);
    }

    /**
     * Retrieves a 128-bit decimal value stored at the specified key.
     * Returns the value as a plain decimal string, or NULL if the key does not exist.
     *
     * @param key the key whose value should be retrieved; must not be null
     * @return a {@link Command} instance representing the ZGET.D128 operation,
     * containing the value as a String, or null if the key doesn't exist
     */
    public Command<K, V, V> zgetd128(K key) {
        CommandArgs<K, V> args = new CommandArgs<>(codec).
                addKey(key);
        return createCommand(CommandType.ZGET_D128, new ValueOutput<>(codec), args);
    }
}
