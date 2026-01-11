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

package com.kronotop.bucket.pipeline;

import com.kronotop.bucket.BSONUtil;
import com.kronotop.bucket.index.SelectorMatcher;
import org.bson.BsonDocument;
import org.bson.BsonValue;

import java.nio.ByteBuffer;

/**
 * Evaluates $elemMatch predicates against BSON documents.
 * Returns true if at least one array element satisfies all conditions in the sub-predicate.
 */
public class ResidualElemMatchNode implements ResidualPredicateNode {
    private final String selector;
    private final ResidualPredicateNode subPredicate;

    public ResidualElemMatchNode(String selector, ResidualPredicateNode subPredicate) {
        this.selector = selector;
        this.subPredicate = subPredicate;
    }

    public String selector() {
        return selector;
    }

    @Override
    public boolean test(ByteBuffer document) {
        BsonValue bsonValue = SelectorMatcher.match(selector, document);

        // Field doesn't exist or is null
        if (bsonValue == null || bsonValue.isNull()) {
            return false;
        }

        // Must be an array
        if (!bsonValue.isArray()) {
            return false;
        }

        // Test each array element against the sub-predicate
        for (BsonValue element : bsonValue.asArray()) {
            if (testElement(element)) {
                return true;
            }
        }

        return false;
    }

    private boolean testElement(BsonValue element) {
        // If the element is a document, serialize and test against sub-predicate
        if (element.isDocument()) {
            BsonDocument elementDoc = element.asDocument();
            ByteBuffer elementBuffer = BSONUtil.toByteBuffer(elementDoc);
            try {
                return subPredicate.test(elementBuffer);
            } finally {
                elementBuffer.rewind();
            }
        }

        // For scalar elements, wrap in a document with an empty key for comparison
        // This handles queries like: { "tags": { "$elemMatch": { "$eq": "urgent" } } }
        BsonDocument wrapper = new BsonDocument("", element);
        ByteBuffer wrapperBuffer = BSONUtil.toByteBuffer(wrapper);
        try {
            return subPredicate.test(wrapperBuffer);
        } finally {
            wrapperBuffer.rewind();
        }
    }
}
