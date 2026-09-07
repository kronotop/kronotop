/*
 * Copyright (c) 2023-2026 Burak Sezer
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


package com.kronotop.internal;

import com.kronotop.Context;
import com.kronotop.KronotopException;
import com.kronotop.cluster.Member;
import com.kronotop.cluster.MemberIdGenerator;
import com.kronotop.cluster.MembershipService;
import com.kronotop.cluster.ShardRegistry;
import com.kronotop.cluster.handlers.InvalidShardIdException;
import com.kronotop.cluster.sharding.ShardKind;
import com.kronotop.server.IllegalCommandArgumentException;
import io.netty.buffer.ByteBuf;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

/**
 * Utility class for reading and interpreting protocol messages from {@code ByteBuf}
 * objects. This class provides methods to parse different data types, such as strings,
 * integers, doubles, booleans, and member IDs, from raw byte buffers.
 */
public class ProtocolMessageUtil {

    /**
     * Expected value description for keywords that take a count.
     */
    public static final String POSITIVE_INTEGER = "a positive integer";

    /**
     * Expected value description for keywords that take a key selector name.
     */
    public static final String VALID_KEY_SELECTOR = "a valid key selector";

    public static byte[] readAsByteArray(ByteBuf buf) {
        byte[] raw = new byte[buf.readableBytes()];
        buf.readBytes(raw);
        return raw;
    }

    /**
     * Reads the content of the provided ByteBuf as a string.
     *
     * @param buf the ByteBuf containing the raw bytes to be read
     * @return a string representation of the bytes in the provided ByteBuf
     */
    public static String readAsString(ByteBuf buf) {
        byte[] raw = new byte[buf.readableBytes()];
        buf.readBytes(raw);
        return new String(raw);
    }

    /**
     * Parses a shard kind from the given ByteBuf.
     *
     * @param shardKindBuf buffer containing the shard kind name (e.g., "STASH", "BUCKET")
     * @return the parsed ShardKind enum value
     * @throws KronotopException if the value is not a valid shard kind
     */
    public static ShardKind readShardKind(ByteBuf shardKindBuf) {
        String rawKind = ProtocolMessageUtil.readAsString(shardKindBuf);
        try {
            return ShardKind.valueOf(rawKind.toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new KronotopException("invalid shard kind");
        }
    }

    /**
     * Reads the content of the provided ByteBuf and interprets it as a double.
     * The method first retrieves the content as a string and then attempts
     * to parse it into a double. If the parsing fails, it throws a KronotopException.
     *
     * @param buf the ByteBuf containing the raw bytes to be interpreted as a double
     * @return the parsed double value from the content of the provided ByteBuf
     * @throws KronotopException if the content cannot be parsed into a double
     */
    public static double readAsDouble(ByteBuf buf) {
        String raw = readAsString(buf);
        try {
            return Double.parseDouble(raw);
        } catch (NumberFormatException e) {
            throw new KronotopException("value is not a double or out of range");
        }
    }

    /**
     * Reads the content of the provided ByteBuf and interprets it as a long value.
     * The method first retrieves the content as a string and then attempts
     * to parse it into a long type. If the parsing fails, it throws a KronotopException.
     *
     * @param buf the ByteBuf containing the raw bytes to be interpreted as a long value
     * @return the parsed long value from the content of the provided ByteBuf
     * @throws KronotopException if the content cannot be parsed into a long
     */
    public static long readAsLong(ByteBuf buf) {
        String raw = readAsString(buf);
        try {
            return Long.parseLong(raw);
        } catch (NumberFormatException e) {
            throw new KronotopException("value is not a long or out of range");
        }
    }

    /**
     * Reads the content of the provided ByteBuf and interprets it as an integer value.
     * The method first retrieves the content as a string and then attempts
     * to parse it into an integer. If the parsing fails, it throws a KronotopException.
     *
     * @param buf the ByteBuf containing the raw bytes to be interpreted as an integer
     * @return the parsed integer value from the content of the provided ByteBuf
     * @throws KronotopException if the content cannot be parsed into an integer or is out of range
     */
    public static int readAsInteger(ByteBuf buf) {
        String raw = readAsString(buf);
        try {
            return Integer.parseInt(raw);
        } catch (NumberFormatException e) {
            throw new KronotopException("value is not a int or out of range");
        }
    }

    /**
     * Reads a boolean value from the provided ByteBuf. The content of the ByteBuf is interpreted
     * as a string and then converted to a Boolean. If the content does not represent a valid boolean
     * value ("TRUE" or "FALSE"), a KronotopException is thrown.
     *
     * @param buf the ByteBuf containing the raw bytes to be interpreted as a boolean value
     * @return the boolean value ("true" for "TRUE", "false" for "FALSE") parsed from the ByteBuf
     * @throws KronotopException if the content does not represent a valid boolean value
     */
    public static Boolean readBooleanValue(ByteBuf buf) {
        String value = ProtocolMessageUtil.readAsString(buf);
        try {
            BooleanValue booleanValue = BooleanValue.valueOf(value.toUpperCase());
            return switch (booleanValue) {
                case TRUE -> true;
                case FALSE -> false;
            };
        } catch (IllegalArgumentException e) {
            throw new KronotopException("Invalid boolean value: " + value);
        }
    }

    /**
     * Finds a member from the list of members in the provided context whose ID starts with the given prefix.
     * If no member is found or if multiple members match the prefix, an exception is thrown.
     *
     * @param context      the context containing the membership information
     * @param memberPrefix the prefix string used to identify the member
     * @return the member whose ID starts with the given prefix
     * @throws KronotopException if no member or more than one member is found with the given prefix
     */
    private static Member findMemberWithPrefix(Context context, String memberPrefix) {
        MembershipService membership = context.getService(MembershipService.NAME);
        assert membership != null;

        Set<Member> result = new HashSet<>();
        TreeSet<Member> members = membership.listMembers();
        for (Member member : members) {
            if (member.getId().startsWith(memberPrefix)) {
                result.add(member);
            }
        }
        if (result.isEmpty()) {
            throw new KronotopException("no member found with prefix: " + memberPrefix);
        }
        if (result.size() > 1) {
            throw new KronotopException("more than one member found with prefix: " + memberPrefix);
        }
        return result.iterator().next();
    }

    /**
     * Reads a member ID from the provided ByteBuf. If the member ID extracted from the buffer
     * has a length of 4, it attempts to find a matching member based on the prefix. For other
     * lengths, it validates the member ID and returns it if valid. Throws an exception if
     * the member ID is invalid.
     *
     * @param context     the context from which the member information is retrieved
     * @param memberIdBuf the ByteBuf containing the raw bytes representing the member ID
     * @return the resolved member ID if valid
     * @throws KronotopException if the member ID is invalid or cannot be resolved
     */
    public static String readMemberId(Context context, ByteBuf memberIdBuf) {
        String memberId = readAsString(memberIdBuf);
        if (memberId.length() == 4) {
            Member member = findMemberWithPrefix(context, memberId);
            return member.getId();
        }
        // Validate the member id.
        if (MemberIdGenerator.validateId(memberId)) {
            return memberId;
        } else {
            throw new KronotopException("Invalid memberId: " + memberId);
        }
    }

    /**
     * Parses and validates a shard ID from the given ByteBuf.
     *
     * @param registry   the shard registry for validation
     * @param shardKind  the shard kind to validate against
     * @param shardIdBuf buffer containing the shard ID as a numeric string
     * @return the validated shard ID
     * @throws InvalidShardIdException if the value is not a valid integer or out of range
     */
    public static int readShardId(ShardRegistry registry, ShardKind shardKind, ByteBuf shardIdBuf) {
        String rawShardId = ProtocolMessageUtil.readAsString(shardIdBuf);
        return readShardId(registry, shardKind, rawShardId);
    }

    /**
     * Parses and validates a shard ID from a string.
     *
     * @param registry   the shard registry for validation
     * @param shardKind  the shard kind to validate against
     * @param rawShardId the shard ID as a numeric string
     * @return the validated shard ID
     * @throws InvalidShardIdException if the value is not a valid integer or out of range
     */
    public static int readShardId(ShardRegistry registry, ShardKind shardKind, String rawShardId) {
        try {
            int shardId = Integer.parseInt(rawShardId);
            if (!registry.isValidShardId(shardKind, shardId)) {
                throw new InvalidShardIdException();
            }
            return shardId;
        } catch (NumberFormatException e) {
            throw new InvalidShardIdException();
        }
    }

    /**
     * Marks a keyword argument as seen and rejects a repeated keyword.
     * <p>
     * The seen set is a bitmask over the enum ordinals. It costs one {@code long} on the stack
     * and two-bit operations per keyword, with no allocation. Callers start with {@code 0} and
     * pass the returned value to the next call.
     *
     * @param seen the current bitmask
     * @param key  the parsed keyword
     * @param name the keyword as it is written on the wire, used in the error message
     * @return the bitmask with the keyword added
     * @throws IllegalCommandArgumentException if the keyword was already seen
     */
    public static long markArgumentSeen(long seen, Enum<?> key, String name) {
        if (key.ordinal() >= Long.SIZE) {
            throw new IllegalStateException("Too many keyword arguments to track: " + key.getClass().getName());
        }
        long bit = 1L << key.ordinal();
        if ((seen & bit) != 0) {
            throw new IllegalCommandArgumentException(String.format("Duplicate '%s' argument", name));
        }
        return seen | bit;
    }

    /**
     * Marks a keyword argument as seen and rejects a repeated keyword, using the enum constant
     * name in the error message.
     *
     * @param seen the current bitmask
     * @param key  the parsed keyword
     * @return the bitmask with the keyword added
     * @throws IllegalCommandArgumentException if the keyword was already seen
     * @see #markArgumentSeen(long, Enum, String)
     */
    public static long markArgumentSeen(long seen, Enum<?> key) {
        return markArgumentSeen(seen, key, key.name());
    }

    /**
     * Returns the value that follows a keyword at index {@code i}.
     *
     * @param params   the command parameters
     * @param i        the index of the keyword
     * @param keyword  the keyword as it is written on the wire, used in the error message
     * @param expected a short description of the accepted value, used in the error message
     * @return the buffer holding the value
     * @throws IllegalCommandArgumentException if the keyword is the last argument
     */
    public static ByteBuf requireValue(List<ByteBuf> params, int i, String keyword, String expected) {
        if (params.size() <= i + 1) {
            throw illegalValue(keyword, expected);
        }
        return params.get(i + 1);
    }

    /**
     * Builds the error for a keyword that is missing its value or carries an unusable one.
     *
     * @param keyword  the keyword as it is written on the wire
     * @param expected a short description of the accepted value
     * @return the exception to throw
     */
    public static IllegalCommandArgumentException illegalValue(String keyword, String expected) {
        return new IllegalCommandArgumentException(
                String.format("%s argument must be followed by %s", keyword, expected)
        );
    }

    /**
     * Represents a boolean value with two possible states: TRUE or FALSE.
     * <p>
     * This enum is designed to provide a strict representation of boolean values for use cases
     * that require controlled handling of boolean-like data, typically when interacting with
     * external systems such as a ByteBuf in the context of parsing values.
     */
    enum BooleanValue {
        TRUE,
        FALSE
    }
}
