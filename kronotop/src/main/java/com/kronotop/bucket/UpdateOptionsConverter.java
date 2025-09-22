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

package com.kronotop.bucket;

import com.kronotop.bucket.pipeline.UpdateOptions;
import org.bson.BsonArray;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.Document;

import java.util.List;

public class UpdateOptionsConverter {

    public static UpdateOptions fromDocument(Document updateDoc) {
        UpdateOptions.Builder builder = UpdateOptions.builder();

        for (String key : updateDoc.keySet()) {
            Object value = updateDoc.get(key);

            switch (key.toLowerCase()) {
                case UpdateOptions.SET -> {
                    if (value instanceof Document setDoc) {
                        // Handle Document format: Document{{likes=2}}
                        for (String field : setDoc.keySet()) {
                            Object fieldValue = setDoc.get(field);
                            if (fieldValue instanceof BsonValue bsonValue) {
                                builder.set(field, bsonValue);
                            } else {
                                builder.set(field, BSONUtil.toBsonValue(fieldValue));
                            }
                        }
                    }
                }
                case UpdateOptions.UNSET -> {
                    if (value instanceof BsonArray bsonArray) {
                        // Handle BsonArray format: BsonArray{values=[BsonString{value='field1'}]}
                        // Check BsonArray first since it also implements List
                        for (BsonValue bsonValue : bsonArray) {
                            if (bsonValue instanceof BsonString bsonString) {
                                builder.unset(bsonString.getValue());
                            } else {
                                throw new IllegalArgumentException("unset key must be a string, got: " + bsonValue.getClass().getSimpleName());
                            }
                        }
                    } else if (value instanceof List<?> unsetKeys) {
                        // Handle List format: [field1, field2] (includes ArrayList, List.of(), etc.)
                        for (Object unsetKey : unsetKeys) {
                            if (!(unsetKey instanceof String)) {
                                throw new IllegalArgumentException("unset key must be a string");
                            }
                            builder.unset((String) unsetKey);
                        }
                    }
                }
            }
        }

        return builder.build();
    }
}