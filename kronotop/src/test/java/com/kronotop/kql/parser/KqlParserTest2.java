/*
 * Copyright (c) 2023-2024 Kronotop
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

package com.kronotop.kql.parser;

import org.bson.BsonReader;
import org.bson.BsonType;
import org.bson.Document;
import org.junit.jupiter.api.Test;

class KqlParserTest2 {

    // { "$or": [ { "status": "A" }, { "qty": { "$lt": 30 } } ] }

    private void traverse(BsonReader reader) {
        switch (reader.getCurrentBsonType()) {
            case STRING:
                System.out.println(reader.readString());
                break;
            case INT32:
                System.out.println(reader.readInt32());
                break;
            case ARRAY:
                readStartArray(reader);
                break;
            case DOCUMENT:
                readStartDocument(reader);
                break;
        }
    }

    private void readStartArray(BsonReader reader) {
        reader.readStartArray();
        while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
            traverse(reader);
        }
        reader.readEndArray();
    }

    private void readStartDocument(BsonReader reader) {
        reader.readStartDocument();
        while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
            System.out.println(reader.readName());
            traverse(reader);
        }
        reader.readEndDocument();
    }

    @Test
    void parsePredicate() {
        String predicate = "{\"status\": {\"$in\": [\"A\", \"D\"]}}";
        //String predicate = "{ \"$or\": [ { \"status\": \"A\" }, { \"qty\": { \"$lt\": 30 } } ] }";
        //String predicate = "{\"field1\": \"value1\", \"field2\": \"value2\", \"field3\": 23, \"field4\": [31, 18, {\"burak\": \"sezer\"}, \"another-string-value\"]}";
        //String predicate = "{\"array\": [12, 23]}";
        Document doc = Document.parse(predicate);
        try (BsonReader reader = doc.toBsonDocument().asBsonReader()) {
            readStartDocument(reader);
        }
    }

    @Test
    void test_parse_AND() {
        String predicate = "{\"status\": {\"$in\": [\"A\", \"D\"]}}";
        Document doc = Document.parse(predicate);
        BsonReader reader = doc.toBsonDocument().asBsonReader();
        reader.readStartDocument();
        System.out.println(reader.getCurrentBsonType());
        System.out.println(reader.readName());
        System.out.println(reader.getCurrentBsonType());
        reader.readStartDocument();
        System.out.println(reader.getCurrentBsonType());
        System.out.println(reader.readName());
        System.out.println(reader.getCurrentBsonType());
        reader.readStartArray();
        System.out.println(reader.readString());
        System.out.println(reader.readString());
        System.out.println(reader.readBsonType());
    }
}