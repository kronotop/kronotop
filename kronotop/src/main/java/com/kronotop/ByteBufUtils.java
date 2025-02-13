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


package com.kronotop;

import com.kronotop.common.KronotopException;
import io.netty.buffer.ByteBuf;

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
