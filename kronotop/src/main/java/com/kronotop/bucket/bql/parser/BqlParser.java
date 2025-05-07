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

/**
 * BqlParser is a utility class for parsing Bucket Query Language (BQL) queries
 * into a structured format represented by a list of {@link BqlOperator} objects.
 * It supports parsing various BSON types such as STRING, INT32, DOUBLE, BOOLEAN,
 * ARRAY, and DOCUMENT, and processes nested structures recursively.
 * <p>
 * This parser facilitates the traversal of BSON documents to extract and
 * interpret operators and their associated values in a BQL query.
 */
public class BqlParser {
    private final String query;
    private final List<BqlOperator> operators = new LinkedList<>();
    private int level = 0;

    // Parser for Bucket Query Language
    BqlParser(String query) {
        this.query = query;
    }

    /**
     * Parses the given BQL query string into a list of {@link BqlOperator} objects. This method
     * processes the query string into a structured format, representing the operators and their
     * associated values within the query.
     *
     * @param query the BQL query string to be parsed
     * @return a list of {@link BqlOperator} objects representing the parsed structure of the query
     */
    public static List<BqlOperator> parse(String query) {
        return new BqlParser(query).parse();
    }

    /**
     * Traverses the BSON structure using the provided {@link BsonReader} and
     * processes the contents to populate the given {@link BqlOperator}. This method
     * handles various BSON types such as STRING, INT32, DOUBLE, BOOLEAN, ARRAY, and DOCUMENT.
     * For ARRAY and DOCUMENT types, it recursively processes nested structures.
     *
     * @param reader   the {@link BsonReader} used to read and parse the BSON structure
     * @param operator the {@link BqlOperator} instance to which the processed values
     *                 are added or nested structures are linked
     */
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

    /**
     * Reads the beginning of a BSON array from the provided {@link BsonReader}, processes all
     * elements within the array, and populates the given {@link BqlOperator} with the parsed
     * elements. This method recursively traverses nested BSON structures such as arrays or
     * documents and processes their contents.
     *
     * @param reader   the {@link BsonReader} from which the BSON array and its elements are read
     * @param operator the {@link BqlOperator} instance to which the parsed elements are added
     */
    private void readStartArray(BsonReader reader, BqlOperator operator) {
        reader.readStartArray();
        level++;
        while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
            traverse(reader, operator);
        }
        level--;
        reader.readEndArray();
    }

    /**
     * Reads the beginning of a BSON document from the provided {@link BsonReader},
     * processes its fields and operators, and adds the corresponding {@link BqlOperator}
     * instances to the operator list. The method traverses through each field in the
     * document to identify supported operators or throws an exception for unknown ones.
     * Recursively processes nested BSON structures.
     *
     * @param reader the {@link BsonReader} from which the BSON document and its
     *               contents are read
     * @throws BqlParserException if an unknown operator is encountered
     */
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
                default -> {
                    if (field.startsWith("$")) {
                        throw new BqlParserException(String.format("Unknown operator: %s", field));
                    }
                    yield new BqlEqOperator(level, field);
                }
            };
            operators.add(bqlOperator);
            traverse(reader, bqlOperator);
        }
        level--;
        reader.readEndDocument();
    }

    /**
     * Parses the internal query string into a list of {@link BqlOperator} objects
     * representing the structure and components of a BQL query. This method
     * converts the query into a BSON document, initiates BSON document reading,
     * and processes the BSON structure to populate the list of operators.
     * The method ensures that the resulting list of operators is immutable.
     *
     * @return an unmodifiable list of {@link BqlOperator} objects parsed from the
     * BQL query.
     */
    private List<BqlOperator> parse() {
        Document document = Document.parse(query);
        try (BsonReader reader = document.toBsonDocument().asBsonReader()) {
            readStartDocument(reader);
        }
        return Collections.unmodifiableList(operators);
    }
}
