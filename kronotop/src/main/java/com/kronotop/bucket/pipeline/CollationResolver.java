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

package com.kronotop.bucket.pipeline;

import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.Collation;
import com.kronotop.bucket.index.CompoundIndex;
import com.kronotop.bucket.index.CompoundIndexField;
import com.kronotop.bucket.index.Index;
import com.kronotop.bucket.index.IndexSelectionPolicy;
import org.bson.BsonType;

import java.util.Collection;

/**
 * Resolves the effective collation for a given field selector.
 */
public class CollationResolver {

    /**
     * Resolves the effective collation for residual predicate evaluation on the given selector.
     * <p>
     * Priority: query-level &gt; single-field index &gt; compound index (unambiguous STRING field)
     * &gt; bucket-level &gt; null (binary comparison).
     *
     * @param metadata       the bucket metadata containing index and bucket-level collation info
     * @param selector       the field selector to resolve collation for
     * @param queryCollation the query-level collation override, or null if not specified
     * @return the resolved Collation, or null if no collation applies (binary comparison)
     */
    public static Collation resolve(BucketMetadata metadata, String selector, Collation queryCollation) {
        if (queryCollation != null) {
            return queryCollation;
        }

        Index index = metadata.indexes().getIndex(selector, IndexSelectionPolicy.READ);
        if (index != null && index.definition().collation() != null) {
            return index.definition().collation();
        }

        Collation compoundCollation = resolveFromCompoundIndexes(metadata, selector);
        if (compoundCollation != null) {
            return compoundCollation;
        }

        return metadata.collation();
    }

    private static Collation resolveFromCompoundIndexes(BucketMetadata metadata, String selector) {
        Collection<CompoundIndex> indexes = metadata.compoundIndexes().getIndexes(IndexSelectionPolicy.READ);
        Collation found = null;
        for (CompoundIndex index : indexes) {
            boolean isStringField = false;
            for (CompoundIndexField field : index.definition().fields()) {
                if (field.selector().equals(selector) && field.bsonType() == BsonType.STRING) {
                    isStringField = true;
                    break;
                }
            }
            if (!isStringField) {
                continue;
            }
            Collation indexCollation = index.definition().collation();
            if (indexCollation == null) {
                continue;
            }
            if (found == null) {
                found = indexCollation;
            } else if (!found.equals(indexCollation)) {
                return null;  // conflicting collations — fall through to bucket
            }
        }
        return found;
    }
}
