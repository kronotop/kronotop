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

import org.bson.BsonType;

import java.math.BigDecimal;

public record Decimal128Val(BigDecimal value) implements BqlValue<BigDecimal> {
    @Override
    public BsonType bsonType() {
        return BsonType.DECIMAL128;
    }

    @Override
    public CustomType customType() {
        return null;
    }
}
