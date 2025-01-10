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
import com.kronotop.kql.operators.impl.comparison.KqlEqOperator;
import com.kronotop.kql.operators.impl.comparison.KqlGtOperator;
import com.kronotop.kql.operators.impl.comparison.KqlLtOperator;
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
                KqlValue<String> stringValue = new KqlValue<>(BsonType.STRING);
                stringValue.setValue(reader.readString());
                operator.setValue(stringValue);
                break;
            case INT32:
                KqlValue<Integer> int32Value = new KqlValue<>(BsonType.INT32);
                int32Value.setValue(reader.readInt32());
                operator.setValue(int32Value);
                break;
            case ARRAY:
                readStartArray(reader, operator);
                break;
            case DOCUMENT:
                readStartDocument(reader);
                break;
        }
    }

    private void readStartArray(BsonReader reader, KqlOperator operator) {
        reader.readStartArray();
        level++;
        while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
            traverse(reader, operator);
        }
        level--;
        reader.readEndArray();
    }

    private void readStartDocument(BsonReader reader) {
        reader.readStartDocument();
        level++;

        while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
            String field = reader.readName();
            String operator = field.toUpperCase();
            KqlOperator kqlOperator = switch (operator) {
                case KqlEqOperator.NAME -> new KqlEqOperator(level);
                case KqlOrOperator.NAME -> new KqlOrOperator(level);
                case KqlLtOperator.NAME -> new KqlLtOperator(level);
                case KqlGtOperator.NAME -> new KqlGtOperator(level);
                case KqlAndOperator.NAME -> new KqlAndOperator(level);
                default -> {
                    KqlEqOperator eqOperator = new KqlEqOperator(level);
                    eqOperator.setField(field);
                    yield eqOperator;
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
