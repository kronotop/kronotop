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


package com.kronotop.internal;

import com.kronotop.Context;
import com.kronotop.cluster.Member;
import com.kronotop.cluster.MemberIdGenerator;
import com.kronotop.cluster.MembershipService;
import com.kronotop.common.KronotopException;
import io.netty.buffer.ByteBuf;

import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;

public class ByteBufUtils {

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
            throw new KronotopException("Error parsing double value: " + raw);
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
        String value = ByteBufUtils.readAsString(buf);
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
