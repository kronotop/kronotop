/*
 * Copyright (c) 2023-2025 Burak Sezer
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.kronotop.bucket.handlers;

import com.kronotop.bucket.UpdateOptionsConverter;
import com.kronotop.bucket.pipeline.UpdateOptions;
import org.bson.*;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class UpdateOptionsConverterTest {

    @Test
    void shouldConvertDocumentToUpdateOptionsWithSetAndUnsetArrayList() {
        BsonDocument updateDoc = new BsonDocument();
        BsonDocument setDoc = new BsonDocument();
        setDoc.append("likes", new BsonInt32(2));
        setDoc.append("name", new BsonString("John"));
        updateDoc.append(UpdateOptions.SET, setDoc);

        BsonArray unsetArray = new BsonArray();
        unsetArray.add(new BsonString("oldField"));
        unsetArray.add(new BsonString("deprecatedField"));
        updateDoc.append(UpdateOptions.UNSET, unsetArray);

        UpdateOptions result = UpdateOptionsConverter.fromDocument(updateDoc);

        assertEquals(2, result.setOps().size());
        assertTrue(result.setOps().containsKey("likes"));
        assertTrue(result.setOps().containsKey("name"));
        assertEquals(new BsonInt32(2), result.setOps().get("likes"));
        assertEquals(new BsonString("John"), result.setOps().get("name"));

        assertEquals(2, result.unsetOps().size());
        assertTrue(result.unsetOps().contains("oldField"));
        assertTrue(result.unsetOps().contains("deprecatedField"));
    }

    @Test
    void shouldConvertDocumentToUpdateOptionsWithBsonArray() {
        BsonDocument updateDoc = new BsonDocument();
        BsonDocument setDoc = new BsonDocument();
        setDoc.append("status", new BsonString("active"));
        updateDoc.append(UpdateOptions.SET, setDoc);

        BsonArray unsetArray = new BsonArray();
        unsetArray.add(new BsonString("tempField"));
        unsetArray.add(new BsonString("cacheField"));
        updateDoc.append(UpdateOptions.UNSET, unsetArray);

        UpdateOptions result = UpdateOptionsConverter.fromDocument(updateDoc);

        assertEquals(1, result.setOps().size());
        assertEquals(new BsonString("active"), result.setOps().get("status"));

        assertEquals(2, result.unsetOps().size());
        assertTrue(result.unsetOps().contains("tempField"));
        assertTrue(result.unsetOps().contains("cacheField"));
    }

    @Test
    void shouldConvertDocumentToUpdateOptionsWithOnlySet() {
        BsonDocument updateDoc = new BsonDocument();
        BsonDocument setDoc = new BsonDocument();
        setDoc.append("count", new BsonInt32(42));
        setDoc.append("enabled", new BsonBoolean(true));
        updateDoc.append(UpdateOptions.SET, setDoc);

        UpdateOptions result = UpdateOptionsConverter.fromDocument(updateDoc);

        assertEquals(2, result.setOps().size());
        assertEquals(new BsonInt32(42), result.setOps().get("count"));
        assertEquals(new BsonBoolean(true), result.setOps().get("enabled"));
        assertTrue(result.unsetOps().isEmpty());
    }

    @Test
    void shouldConvertDocumentToUpdateOptionsWithOnlyUnset() {
        BsonDocument updateDoc = new BsonDocument();
        BsonArray unsetArray = new BsonArray();
        unsetArray.add(new BsonString("field1"));
        unsetArray.add(new BsonString("field2"));
        updateDoc.append(UpdateOptions.UNSET, unsetArray);

        UpdateOptions result = UpdateOptionsConverter.fromDocument(updateDoc);

        assertTrue(result.setOps().isEmpty());
        assertEquals(2, result.unsetOps().size());
        assertTrue(result.unsetOps().contains("field1"));
        assertTrue(result.unsetOps().contains("field2"));
    }

    @Test
    void shouldThrowExceptionForInvalidUnsetKey() {
        BsonDocument updateDoc = new BsonDocument();
        BsonArray unsetArray = new BsonArray();
        unsetArray.add(new BsonInt32(123));
        unsetArray.add(new BsonString("validField"));
        updateDoc.append(UpdateOptions.UNSET, unsetArray);

        assertThrows(IllegalArgumentException.class, () -> {
            UpdateOptionsConverter.fromDocument(updateDoc);
        });
    }
}
