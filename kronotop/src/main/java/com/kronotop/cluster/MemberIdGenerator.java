/*
 * Copyright (c) 2023-2024 Kronotop
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

package com.kronotop.cluster;

import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

public class MemberIdGenerator {
    public static int LENGTH = 40;

    public static String generateId() {
        // The member ID, a 40-character globally unique string
        // generated when a node is created and never changed again
        UUID uuid = UUID.randomUUID();
        HashCode hashCode = Hashing.sha1().newHasher().
                putString(uuid.toString(), StandardCharsets.UTF_8).
                hash();
        return hashCode.toString();
    }

    public static boolean validateId(String id) {
        return id.length() == LENGTH;
    }
}
