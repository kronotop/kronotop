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

import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonValue;

/**
 * Helper class to simplify BsonDocument value extraction in tests.
 */
public class BsonHelper {

    public static String getString(BsonDocument doc, String key) {
        BsonValue value = doc.get(key);
        if (value == null || value.isNull()) return null;
        return value.asString().getValue();
    }

    public static Integer getInteger(BsonDocument doc, String key) {
        BsonValue value = doc.get(key);
        if (value == null || value.isNull()) return null;
        return value.asInt32().getValue();
    }

    public static Long getLong(BsonDocument doc, String key) {
        BsonValue value = doc.get(key);
        if (value == null || value.isNull()) return null;
        return value.asInt64().getValue();
    }

    public static Double getDouble(BsonDocument doc, String key) {
        BsonValue value = doc.get(key);
        if (value == null || value.isNull()) return null;
        return value.asDouble().getValue();
    }

    public static Boolean getBoolean(BsonDocument doc, String key) {
        BsonValue value = doc.get(key);
        if (value == null || value.isNull()) return null;
        return value.asBoolean().getValue();
    }

    public static BsonArray getArray(BsonDocument doc, String key) {
        BsonValue value = doc.get(key);
        if (value == null || value.isNull()) return null;
        return value.asArray();
    }

    public static BsonDocument getDocument(BsonDocument doc, String key) {
        BsonValue value = doc.get(key);
        if (value == null || value.isNull()) return null;
        return value.asDocument();
    }

    public static Long getDateTime(BsonDocument doc, String key) {
        BsonValue value = doc.get(key);
        if (value == null || value.isNull()) return null;
        return value.asDateTime().getValue();
    }
}
