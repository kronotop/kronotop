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
import io.lettuce.core.output.*;
import io.lettuce.core.protocol.Command;
import io.lettuce.core.protocol.CommandArgs;

import java.util.List;

/**
 * Provides methods for constructing commands for the ZMap ordered key-value map service.
 * Supports basic key-value operations, range queries, mutations, and typed numeric
 * operations (I64, F64, D128).
 *
 * @param <K> the type of keys handled by this command builder
 * @param <V> the type of values handled by this command builder
 */
public class ZMapCommandBuilder<K, V> extends BaseKronotopCommandBuilder<K, V> {

    public ZMapCommandBuilder(RedisCodec<K, V> codec) {
        super(codec);
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

    /**
     * Sets a 64-bit integer value at the specified key, overwriting any existing value.
     * The value is stored in the same binary format as ZINC.I64.
     *
     * @param key   the key to set; must not be null
     * @param value the 64-bit integer value to store
     * @return a {@link Command} instance representing the ZSET.I64 operation
     */
    public Command<K, V, String> zseti64(K key, long value) {
        CommandArgs<K, V> args = new CommandArgs<>(codec).
                addKey(key).
                add(value);
        return createCommand(CommandType.ZSET_I64, new StatusOutput<>(codec), args);
    }

    /**
     * Sets a 64-bit floating point value at the specified key, overwriting any existing value.
     * The value is stored in the same binary format as ZINC.F64.
     *
     * @param key   the key to set; must not be null
     * @param value the double value to store (must be finite)
     * @return a {@link Command} instance representing the ZSET.F64 operation
     */
    public Command<K, V, String> zsetf64(K key, double value) {
        CommandArgs<K, V> args = new CommandArgs<>(codec).
                addKey(key).
                add(value);
        return createCommand(CommandType.ZSET_F64, new StatusOutput<>(codec), args);
    }

    /**
     * Sets a 128-bit decimal value at the specified key, overwriting any existing value.
     * The value is stored in the same binary format as ZINC.D128.
     *
     * @param key   the key to set; must not be null
     * @param value the decimal value as a string
     * @return a {@link Command} instance representing the ZSET.D128 operation
     */
    public Command<K, V, String> zsetd128(K key, String value) {
        CommandArgs<K, V> args = new CommandArgs<>(codec).
                addKey(key).
                add(value);
        return createCommand(CommandType.ZSET_D128, new StatusOutput<>(codec), args);
    }
}
