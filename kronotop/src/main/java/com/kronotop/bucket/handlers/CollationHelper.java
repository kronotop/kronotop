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

package com.kronotop.bucket.handlers;

import com.kronotop.KronotopException;
import com.kronotop.bucket.Collation;
import com.kronotop.internal.JSONUtil;
import tools.jackson.databind.DatabindException;
import tools.jackson.databind.JsonNode;

import java.util.Set;

public class CollationHelper {
    private static final Set<String> KNOWN_FIELDS = Set.of(
            "locale", "strength", "case_level", "case_first",
            "numeric_ordering", "alternate", "backwards", "normalization",
            "max_variable"
    );

    public static Collation deserializeAndValidate(byte[] data) {
        try {
            JsonNode tree = JSONUtil.objectMapper.readTree(data);
            for (String field : tree.propertyNames()) {
                if (!KNOWN_FIELDS.contains(field)) {
                    throw new KronotopException("Unknown collation field: " + field);
                }
            }

            Collation collation = JSONUtil.readValue(data, Collation.class);
            collation.validate();
            return collation;
        } catch (KronotopException e) {
            if (e.getCause() instanceof DatabindException) {
                throw new KronotopException("Invalid collation specification");
            }
            if (e.getCause() instanceof IllegalArgumentException illegalArgumentException) {
                throw new KronotopException(illegalArgumentException);
            }
            throw e;
        }
    }
}
