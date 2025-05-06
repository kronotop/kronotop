// Copyright (C) 2025 Burak Sezer
// Use of this software is governed by the Business Source License included
// in the LICENSE.TXT file and at www.mariadb.com/bsl11.

// Change Date: 5 years after release

// On the date above, in accordance with the Business Source License,
// use of this software will be governed by the open source license specified
// in the LICENSE.TXT file.

package com.kronotop.bucket.bql.parser;

import com.kronotop.bucket.bql.operators.BqlOperator;
import com.kronotop.bucket.bql.operators.array.BqlAllOperator;
import com.kronotop.bucket.bql.operators.array.BqlElemMatchOperator;
import com.kronotop.bucket.bql.operators.array.BqlSizeOperator;
import com.kronotop.bucket.bql.operators.comparison.*;
import com.kronotop.bucket.bql.operators.element.BqlExistsOperator;
import com.kronotop.bucket.bql.operators.logical.BqlAndOperator;
import com.kronotop.bucket.bql.operators.logical.BqlNorOperator;
import com.kronotop.bucket.bql.operators.logical.BqlNotOperator;
import com.kronotop.bucket.bql.operators.logical.BqlOrOperator;
import com.kronotop.bucket.bql.values.*;
import org.bson.BsonReader;
import org.bson.BsonType;
import org.bson.Document;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

// Parser for Bucket Query Language
public class BqlParser {
    private final String query;
    private final List<BqlOperator> operators = new LinkedList<>();
    private int level = 0;

    BqlParser(String query) {
        this.query = query;
    }

    public static List<BqlOperator> parse(String query) {
        return new BqlParser(query).parse();
    }

    private void traverse(BsonReader reader, BqlOperator operator) {
        switch (reader.getCurrentBsonType()) {
            case STRING:
                BqlValue<String> stringValue = new StringVal(reader.readString());
                operator.addValue(stringValue);
                break;
            case INT32:
                BqlValue<Integer> int32Value = new Int32Val(reader.readInt32());
                operator.addValue(int32Value);
                break;
            case DOUBLE:
                BqlValue<Double> doubleValue = new DoubleVal(reader.readDouble());
                operator.addValue(doubleValue);
                break;
            case BOOLEAN:
                BqlValue<Boolean> booleanValue = new BooleanVal(reader.readBoolean());
                operator.addValue(booleanValue);
                break;
            case ARRAY:
                readStartArray(reader, operator);
                break;
            case DOCUMENT:
                readStartDocument(reader);
                break;
        }
    }

    private void readStartArray(BsonReader reader, BqlOperator operator) {
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
            BqlOperator bqlOperator = switch (operator) {
                case BqlEqOperator.NAME -> new BqlEqOperator(level);
                case BqlAllOperator.NAME -> new BqlAllOperator(level);
                case BqlOrOperator.NAME -> new BqlOrOperator(level);
                case BqlLtOperator.NAME -> new BqlLtOperator(level);
                case BqlGtOperator.NAME -> new BqlGtOperator(level);
                case BqlNinOperator.NAME -> new BqlNinOperator(level);
                case BqlNotOperator.NAME -> new BqlNotOperator(level);
                case BqlGteOperator.NAME -> new BqlGteOperator(level);
                case BqlLteOperator.NAME -> new BqlLteOperator(level);
                case BqlNeOperator.NAME -> new BqlNeOperator(level);
                case BqlInOperator.NAME -> new BqlInOperator(level);
                case BqlAndOperator.NAME -> new BqlAndOperator(level);
                case BqlNorOperator.NAME -> new BqlNorOperator(level);
                case BqlSizeOperator.NAME -> new BqlSizeOperator(level);
                case BqlElemMatchOperator.NAME -> new BqlElemMatchOperator(level);
                case BqlExistsOperator.NAME -> new BqlExistsOperator(level);
                default -> new BqlEqOperator(level, field);
            };
            operators.add(bqlOperator);
            traverse(reader, bqlOperator);
        }
        level--;
        reader.readEndDocument();
    }

    private List<BqlOperator> parse() {
        Document document = Document.parse(query);
        try (BsonReader reader = document.toBsonDocument().asBsonReader()) {
            readStartDocument(reader);
        }
        return Collections.unmodifiableList(operators);
    }
}
