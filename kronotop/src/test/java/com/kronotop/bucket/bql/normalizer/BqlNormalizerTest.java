// Copyright (C) 2025 Burak Sezer
// Use of this software is governed by the Business Source License included
// in the LICENSE.TXT file and at www.mariadb.com/bsl11.

// Change Date: 5 years after release

// On the date above, in accordance with the Business Source License,
// use of this software will be governed by the open source license specified
// in the LICENSE.TXT file.

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