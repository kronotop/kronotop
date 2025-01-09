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

import com.kronotop.kql.KqlValue;
import com.kronotop.kql.operators.KqlOperator;
import com.kronotop.kql.operators.impl.logical.*;
import org.bson.BsonReader;
import org.bson.BsonType;
import org.bson.Document;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

public class KqlParser {
    private final String query;
    private final List<KqlOperator> operators = new LinkedList<>();
    private int level = 0;

    KqlParser(String query) {
        this.query = query;
    }

    public static List<KqlOperator> parse(String query) {
        return new KqlParser(query).parse();
    }

    private void traverse(BsonReader reader, KqlOperator operator) {
        switch (reader.getCurrentBsonType()) {
            case STRING:
                KqlValue value = new KqlValue(BsonType.STRING);
                value.setStringValue(reader.readString());
                operator.setValue(value);
                break;
            case INT32:
                KqlValue valueint32 = new KqlValue(BsonType.INT32);
                valueint32.setInt32Value(reader.readInt32());
                operator.setValue(valueint32);
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
        level++;
        while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
            traverse(reader, null);
        }
        level--;
        reader.readEndArray();
    }

    private void readStartDocument(BsonReader reader) {
        System.out.println("Reading document");
        reader.readStartDocument();
        level++;
        while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
            String name = reader.readName();
            String operator = name.toUpperCase();
            KqlOperator kqlOperator = switch (operator) {
                case KqlEqOperator.NAME -> {
                    System.out.println("EQ Operator, level: " + level);
                    yield new KqlEqOperator(level);
                }
                case KqlOrOperator.NAME -> {
                    System.out.println("OR OPERATOR, level: " + level);
                    yield new KqlOrOperator(level);
                }
                case KqlLtOperator.NAME -> {
                    System.out.println("LT OPERATOR, level: " + level);
                    yield new KqlLtOperator(level);
                }
                case KqlGtOperator.NAME -> {
                    System.out.println("GT OPERATOR, level: " + level);
                    yield new KqlGtOperator(level);
                }
                case KqlAndOperator.NAME -> {
                    System.out.println("AND OPERATOR, level: " + level);
                    yield new KqlAndOperator(level);
                }
                default -> {
                    System.out.println("EQ OPERATOR, level: " + level);
                    KqlEqOperator op = new KqlEqOperator(level);
                    op.setField(name);
                    yield op;
                }
            };
            operators.add(kqlOperator);
            traverse(reader, kqlOperator);
        }
        level--;
        reader.readEndDocument();
    }

    private List<KqlOperator> parse() {
        Document document = Document.parse(query);
        try (BsonReader reader = document.toBsonDocument().asBsonReader()) {
            readStartDocument(reader);
        }
        return Collections.unmodifiableList(operators);
    }
}
