package com.kronotop.bql.parser;

import com.kronotop.bql.BqlValue;
import com.kronotop.bql.operators.BqlOperator;
import com.kronotop.bql.operators.array.BqlAllOperator;
import com.kronotop.bql.operators.comparison.BqlEqOperator;
import com.kronotop.bql.operators.comparison.BqlLtOperator;
import com.kronotop.bql.operators.comparison.BqlNinOperator;
import com.kronotop.bql.operators.logical.BqlOrOperator;
import org.bson.BsonReader;
import org.bson.BsonType;
import org.bson.Document;

import java.util.LinkedList;
import java.util.List;

public class BqlParser {
    private final String query;
    private int level = 0;
    private final List<BqlOperator> operators = new LinkedList<>();

    BqlParser(String query) {
        this.query = query;
    }

    public static List<BqlOperator> parse(String query) {
        return new BqlParser(query).parse();
    }

    private void traverse(BsonReader reader, BqlOperator operator) {
        switch (reader.getCurrentBsonType()) {
            case STRING:
                BqlValue<String> stringValue = new BqlValue<>(BsonType.STRING);
                stringValue.setValue(reader.readString());
                operator.addValue(stringValue);
                break;
            case INT32:
                BqlValue<Integer> int32Value = new BqlValue<>(BsonType.INT32);
                int32Value.setValue(reader.readInt32());
                operator.addValue(int32Value);
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
                case BqlNinOperator.NAME -> new BqlNinOperator(level);
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
        return operators;
    }
}
