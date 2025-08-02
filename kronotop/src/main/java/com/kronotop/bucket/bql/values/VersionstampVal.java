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

package com.kronotop.bucket.bql.values;

import com.apple.foundationdb.tuple.Versionstamp;
import org.bson.BsonType;

public record VersionstampVal(Versionstamp vs) implements BqlValue<Versionstamp> {
    public static final CustomType TYPE = new CustomType((byte) 0x80, "VERSIONSTAMP");

    @Override
    public Versionstamp value() {
        return vs;
    }

    @Override
    public BsonType bsonType() {
        return null;
    }

    @Override
    public CustomType customType() {
        return TYPE;
    }
}