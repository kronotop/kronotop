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

package com.kronotop.stash.storage;

import com.kronotop.stash.handlers.hash.HashFieldValue;
import com.kronotop.stash.handlers.hash.HashValue;
import com.kronotop.stash.handlers.string.StringValue;

public class StashValueContainer {
    private final StashValueKind kind;
    private StringValue stringValue;
    private HashValue hashValue;
    private HashFieldValue hashFieldValue;

    public StashValueContainer(StringValue value) {
        this.kind = StashValueKind.STRING;
        this.stringValue = value;
    }

    public StashValueContainer(HashValue value) {
        this.kind = StashValueKind.HASH;
        this.hashValue = value;
    }

    public StashValueContainer(HashFieldValue value) {
        this.kind = StashValueKind.HASH_FIELD;
        this.hashFieldValue = value;
    }

    public StashValueKind kind() {
        return kind;
    }

    public StringValue string() {
        return stringValue;
    }

    public HashValue hash() {
        return hashValue;
    }

    public HashFieldValue hashField() {
        return hashFieldValue;
    }

    public BaseStashValue<?> baseStashValue() {
        return switch (kind) {
            case STRING -> string();
            case HASH_FIELD -> hashFieldValue;
            default -> throw new UnsupportedOperationException();
        };
    }
}
