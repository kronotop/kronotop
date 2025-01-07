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

import com.kronotop.kql.KqlNode;
import com.kronotop.kql.operators.impl.logical.OrOperator;
import org.bson.BsonReader;
import org.bson.BsonType;
import org.bson.Document;

public class KqlParser {
    private final String query;

    KqlParser(String query) {
        this.query = query;
    }

    private void traverse(BsonReader reader) {
        switch (reader.getCurrentBsonType()) {
            case STRING:
                System.out.println("$$ STRING " + reader.readString());
                break;
            case INT32:
                System.out.println("$$ INT32 " + reader.readInt32());
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
        System.out.println("Reading array");
        reader.readStartArray();
        while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
            traverse(reader);
        }
        reader.readEndArray();
    }

    private void readStartDocument(BsonReader reader) {
        System.out.println("Reading document");
        reader.readStartDocument();
        while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
            String operator = reader.readName().toUpperCase();
            switch (operator) {
                case OrOperator.NAME:
                    System.out.println("OR OPERATOR");
            }
            traverse(reader);
        }
        reader.readEndDocument();
    }

    private KqlNode parse() {
        Document document = Document.parse(query);
        try (BsonReader reader = document.toBsonDocument().asBsonReader()) {
            readStartDocument(reader);
        }
        return new KqlNode();
    }

    public static KqlNode parse(String query) {
        return new KqlParser(query).parse();
    }
}
