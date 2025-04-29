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

import io.lettuce.core.protocol.ProtocolKeyword;

import java.nio.charset.StandardCharsets;

/**
 * Defines a set of keywords for namespace-related commands in the Kronotop system.
 * These keywords are used for operations such as creating, using, and managing namespaces.
 *
 * Each keyword is represented as a protocol keyword and backed by its name, converted
 * to a byte array using ASCII encoding for efficient usage in command processing.
 *
 * Enum Constants:
 * - CREATE: Represents the "CREATE" operation for namespaces.
 * - USE: Represents the "USE" operation to switch to a specific namespace.
 * - CURRENT: Represents the "CURRENT" operation to retrieve the current namespace.
 * - LIST: Represents the "LIST" operation to list available namespaces.
 * - MOVE: Represents the "MOVE" operation to rename or relocate a namespace.
 * - REMOVE: Represents the "REMOVE" operation to delete a namespace.
 * - EXISTS: Represents the "EXISTS" operation to check the existence of a namespace.
 * - LAYER: Represents the "LAYER" keyword for namespace configuration.
 * - PREFIX: Represents the "PREFIX" keyword for namespace configuration.
 */
public enum NamespaceKeywords implements ProtocolKeyword {
    CREATE,
    USE,
    CURRENT,
    LIST,
    MOVE,
    REMOVE,
    EXISTS,
    LAYER,
    PREFIX;
    public final byte[] bytes;

    NamespaceKeywords() {
        bytes = name().getBytes(StandardCharsets.US_ASCII);
    }

    @Override
    public byte[] getBytes() {
        return bytes;
    }
}
