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

package com.kronotop.bucket.bql.normalizer;

import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.bucket.bql.operators.BqlOperator;
import com.kronotop.bucket.bql.operators.comparison.BqlGteOperator;
import com.kronotop.bucket.bql.parser.BqlParser;
import com.kronotop.bucket.bql.values.BqlValue;
import com.kronotop.bucket.bql.values.VersionstampVal;
import com.kronotop.internal.VersionstampUtil;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class BqlNormalizerTest {

    @SuppressWarnings("unchecked")
    @Test
    void should_transform_id_field_to_versionstamp() {
        List<BqlOperator> operators = BqlParser.parse("{ _id: { $gte: \"00000U44O1LKI000000Gxxxx\" } }");

        BqlNormalizer.normalize(operators);

        for (BqlOperator operator : operators) {
            if (operator instanceof BqlGteOperator) {
                BqlValue<?> rawValue = operator.getValues().getFirst();
                BqlValue<Versionstamp> value = (BqlValue<Versionstamp>) rawValue;
                assertNull(value.bsonType());
                assertEquals(VersionstampVal.TYPE, value.customType());
                assertEquals(Versionstamp.class, value.value().getClass());
                assertEquals("00000U44O1LKI000000Gxxxx", VersionstampUtil.base32HexEncode(value.value()));
            }
        }
    }
}