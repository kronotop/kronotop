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

package com.kronotop.bucket;

import com.kronotop.KronotopException;
import com.kronotop.server.RESPError;
import org.bson.types.ObjectId;

import java.util.HexFormat;
import java.util.List;
import java.util.stream.Collectors;

public class DuplicateKeyException extends KronotopException {
    public DuplicateKeyException(ObjectId objectId) {
        super(RESPError.DUPLICATEKEY, String.format("Duplicate key: _id '%s'", objectId.toHexString()));
    }

    public DuplicateKeyException(String selector, Object value) {
        super(RESPError.DUPLICATEKEY, String.format("Duplicate key: '%s' %s", selector, render(value)));
    }

    /**
     * Renders an index value for the error message. Binary values become a hex string instead of the
     * default {@code [B@hash}, and a compound key's value list is rendered element by element.
     */
    private static String render(Object value) {
        if (value instanceof byte[] bytes) {
            return "0x" + HexFormat.of().formatHex(bytes);
        }
        if (value instanceof List<?> list) {
            return list.stream().map(DuplicateKeyException::render).collect(Collectors.joining(", ", "[", "]"));
        }
        return String.valueOf(value);
    }
}
