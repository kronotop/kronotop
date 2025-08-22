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

package com.kronotop.bucket.bql;

import com.kronotop.bucket.bql.ast.*;
import com.kronotop.internal.VersionstampUtil;
import org.bson.BsonReader;
import org.bson.BsonType;
import org.bson.Document;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * BqlParser is a utility class designed for parsing Bucket Query Language (BQL) expressions.
 * It provides methods to parse and interpret BSON documents and strings into a structured
 * representation expressed as {@code BqlExpr} objects. The class handles various BQL features,
 * including selector-level expressions, logical operators, and special operators like `$elemMatch`.
 * <p>
 * This class includes high-level static methods for parsing BQL strings, as well as private
 * methods for detailed BSON reading and expression construction.
 * <p>
 * Notable functionality includes:
 * - Parsing BQL strings into {@code BqlExpr} objects.
 * - Processing BSON documents to build query expressions.
 * - Supporting a wide range of operators, such as `$and`, `$or`, `$not`, and `$elemMatch`.
 * - Handling selector-specific operators like `$gt`, `$lt`, `$eq`, `$in`, and `$exists`.
 */
public class BqlParser {
    public static String explain(BqlExpr expr) {
        return Explain.explain(expr, 0);
    }

    /**
     * Parses a given BQL (Bucket Query Language) query string and converts it into a corresponding
     * {@code BqlExpr} representation.
     * <p>
     * The input string is first validated as BSON and then processed for its BQL structure
     * using internal parsing mechanisms. If the input is malformed or contains unsupported
     * operators, a {@code BqlParseException} is thrown.
     *
     * @param query the BQL query string to be parsed
     * @return the parsed {@code BqlExpr} object representing the query
     * @throws BqlParseException if the query string is invalid or cannot be parsed
     */
    public static BqlExpr parse(String query) {
        Document document;
        try {
            document = Document.parse(query);
        } catch (Exception e) {
            throw new BqlParseException("Invalid BSON format", e);
        }

        try (BsonReader reader = document.toBsonDocument().asBsonReader()) {
            return new BqlParser().parse(reader);
        } catch (BqlParseException e) {
            // Re-throw our own exceptions as-is
            throw e;
        } catch (Exception e) {
            // Only catch BSON processing errors and other non-BQL exceptions
            throw new BqlParseException("BSON processing error", e);
        }
    }

    private BqlExpr parse(BsonReader reader) {
        return readExpr(reader);
    }

    /**
     * Reads and parses an expression from the provided {@code BsonReader}, constructing
     * a {@code BqlExpr} object from the BSON document structure.
     * <p>
     * The method processes the BSON document, evaluates its fields, and aggregates any
     * expressions found. If the document contains a single expression, that expression
     * is returned directly. If multiple expressions are found, they are combined into
     * a {@code BqlAnd} object.
     *
     * @param reader the {@code BsonReader} instance used to read the BSON document
     * @return the resulting {@code BqlExpr} object representing the parsed expression
     */
    private BqlExpr readExpr(BsonReader reader) {
        reader.readStartDocument();

        List<BqlExpr> expressions = new ArrayList<>();

        while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
            String key = reader.readName();
            expressions.add(parseSelectorOrOperator(reader, key));
        }

        reader.readEndDocument();

        if (expressions.size() == 1) {
            return expressions.getFirst();
        }
        return new BqlAnd(expressions);
    }

    /**
     * Parses a BSON key-value pair, determining whether the key represents a selector or an operator,
     * and constructs the corresponding {@code BqlExpr} object.
     * <p>
     * If the key starts with '$', it is treated as an operator (e.g., "$and", "$or", "$not") and the
     * method processes it accordingly. Otherwise, the key is treated as a selector name, and an appropriate
     * selector expression or equality expression is created based on the BSON type of the value.
     *
     * @param reader the {@code BsonReader} instance used to read the BSON data
     * @param key    the key in the BSON document to be parsed, which may represent either a selector name or an operator
     * @return the resulting {@code BqlExpr} object constructed from the parsed key and its corresponding value
     * @throws BqlParseException if the key starts with '$' but does not match a known operator or if the BSON value type is not supported
     */
    private BqlExpr parseSelectorOrOperator(BsonReader reader, String key) {
        if (key.startsWith("$")) {
            return switch (key) {
                case "$and" -> new BqlAnd(readArray(reader));
                case "$or" -> new BqlOr(readArray(reader));
                case "$not" -> new BqlNot(readExpr(reader));
                default -> throw new BqlParseException("Unknown operator: " + key);
            };
        } else {
            // It's a selector name
            BsonType type = reader.getCurrentBsonType();
            if (type == BsonType.DOCUMENT) {
                return readSelectorExpression(reader, key);
            }
            return new BqlEq(key, readValue(reader));
        }
    }

    /**
     * Parses a BSON key-value pair within the context of an `$elemMatch` expression, determining whether
     * the key represents a selector or an operator, and constructs the corresponding {@code BqlExpr} object.
     * <p>
     * If the key starts with a '$', it is treated as an operator (e.g., "$and", "$or", "$not") and the
     * method processes it accordingly. Otherwise, the key is treated as a selector name, and an appropriate
     * selector expression or equality expression is created based on the BSON type of the value.
     *
     * @param reader            the {@code BsonReader} instance used to read the BSON data
     * @param key               the key in the BSON document to be parsed, which may represent either a selector name or an operator
     * @param elemMatchSelector the parent selector name within the `$elemMatch` context that is being processed
     * @return the resulting {@code BqlExpr} object constructed from the parsed key and its corresponding value
     */
    private BqlExpr parseSelectorOrOperatorInElemMatch(BsonReader reader, String key, String elemMatchSelector) {
        if (key.startsWith("$")) {
            return switch (key) {
                case "$and" -> new BqlAnd(readArray(reader));
                case "$or" -> new BqlOr(readArray(reader));
                case "$not" -> new BqlNot(readExpr(reader));
                default -> parseSelectorOperator(reader, key, elemMatchSelector);
            };
        } else {
            // It's a selector name
            BsonType type = reader.getCurrentBsonType();
            if (type == BsonType.DOCUMENT) {
                return readSelectorExpression(reader, key);
            }
            return new BqlEq(key, readValue(reader));
        }
    }

    /**
     * Parses a selector operator along with its arguments from a BSON document and creates
     * a corresponding {@code BqlExpr} object based on the provided operator and selector name.
     * <p>
     * Supported operators include:
     * - Comparison operators such as "$gt", "$lt", "$gte", "$lte", "$eq", "$ne"
     * - Array operators such as "$in", "$nin", "$all"
     * - Special operators like "$size" and "$exists"
     * <p>
     * If the operator is not recognized or the value type is invalid for the operator,
     * a {@code BqlParseException} is thrown.
     *
     * @param reader   the {@code BsonReader} instance used to read the BSON data
     * @param op       the operator being parsed (e.g., "$gt", "$eq", "$exists")
     * @param selector the name of the selector the operator applies to
     * @return the resulting {@code BqlExpr} object corresponding to the parsed operator and selector value
     * @throws BqlParseException if the operator is unknown, or if the value type is invalid for the operator
     */
    private BqlExpr parseSelectorOperator(BsonReader reader, String op, String selector) {
        return switch (op) {
            case "$gt" -> new BqlGt(selector, readValue(reader));
            case "$lt" -> new BqlLt(selector, readValue(reader));
            case "$eq" -> new BqlEq(selector, readValue(reader));
            case "$gte" -> new BqlGte(selector, readValue(reader));
            case "$lte" -> new BqlLte(selector, readValue(reader));
            case "$ne" -> new BqlNe(selector, readValue(reader));
            case "$in" -> new BqlIn(selector, readValueArray(reader));
            case "$nin" -> new BqlNin(selector, readValueArray(reader));
            case "$all" -> new BqlAll(selector, readValueArray(reader));
            case "$size" -> {
                if (reader.getCurrentBsonType() != BsonType.INT32) {
                    throw new BqlParseException("$size expects an integer");
                }
                yield new BqlSize(selector, reader.readInt32());
            }
            case "$exists" -> {
                boolean exists = switch (reader.getCurrentBsonType()) {
                    case BOOLEAN -> reader.readBoolean();
                    default -> throw new BqlParseException("$exists expects a boolean");
                };
                yield new BqlExists(selector, exists);
            }
            default -> throw new BqlParseException("Unknown selector operator: " + op);
        };
    }

    /**
     * Reads a selector-based expression from the provided {@code BsonReader} and constructs a {@code BqlExpr}
     * object representing the parsed expression. The method processes selector-level operator documents
     * within BSON data and creates the corresponding query expressions.
     * <p>
     * If the document is empty, the method throws a {@code BqlParseException}. Depending on the selector operator,
     * the method may handle special cases like `$elemMatch` and `$not`. If multiple operators are present
     * in the same selector document, they are combined with an implicit `$and`.
     *
     * @param reader   the {@code BsonReader} instance used to read the BSON document
     * @param selector the name of the selector the expression applies to
     * @return the resulting {@code BqlExpr} object representing the selector expression
     * @throws BqlParseException if the selector operator document is empty or invalid
     */
    private BqlExpr readSelectorExpression(BsonReader reader, String selector) {
        reader.readStartDocument();

        if (reader.readBsonType() == BsonType.END_OF_DOCUMENT) {
            reader.readEndDocument();
            throw new BqlParseException("Empty selector operator document for: " + selector);
        }

        List<BqlExpr> expressions = new ArrayList<>();

        // Process all operators in the selector document
        do {
            String op = reader.readName();
            BqlExpr expr = switch (op) {
                case "$elemMatch" -> new BqlElemMatch(selector, readElemMatchExpr(reader, selector));
                case "$not" -> new BqlNot(readSelectorExpression(reader, selector));
                default -> parseSelectorOperator(reader, op, selector);
            };
            expressions.add(expr);
        } while (reader.readBsonType() != BsonType.END_OF_DOCUMENT);

        reader.readEndDocument();

        // If only one expression, return it directly
        if (expressions.size() == 1) {
            return expressions.getFirst();
        }

        // Multiple expressions are combined with implicit AND
        return new BqlAnd(expressions);
    }

    /**
     * Reads and parses an `$elemMatch` expression from the provided {@code BsonReader}, constructing
     * a {@code BqlExpr} object based on the content of the BSON document.
     * <p>
     * The method processes the document as a series of nested expressions or operators. If the document
     * contains only one expression, it directly returns that expression. If multiple expressions are
     * present, they are combined into a {@code BqlAnd} object.
     *
     * @param reader            the {@code BsonReader} instance used to read the BSON document
     * @param elemMatchSelector the parent selector name associated with the `$elemMatch` context
     * @return a {@code BqlExpr} object representing the parsed `$elemMatch` expression
     */
    private BqlExpr readElemMatchExpr(BsonReader reader, String elemMatchSelector) {
        reader.readStartDocument();

        List<BqlExpr> expressions = new ArrayList<>();

        while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
            String key = reader.readName();
            expressions.add(parseSelectorOrOperatorInElemMatch(reader, key, elemMatchSelector));
        }

        reader.readEndDocument();

        if (expressions.size() == 1) {
            return expressions.getFirst();
        }
        return new BqlAnd(expressions);
    }

    /**
     * Reads an array from the provided {@code BsonReader}, parsing each element
     * into a corresponding {@code BqlExpr} object and returning the results as a list.
     * <p>
     * The method assumes that the reader is positioned at the start of an array
     * and processes each BSON element until reaching the end of the array.
     *
     * @param reader the {@code BsonReader} instance used to read the BSON array
     * @return a list of {@code BqlExpr} objects parsed from the BSON array
     */
    private List<BqlExpr> readArray(BsonReader reader) {
        reader.readStartArray();
        List<BqlExpr> list = new ArrayList<>();

        while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
            list.add(readExpr(reader));
        }

        reader.readEndArray();
        return list;
    }

    /**
     * Reads and parses an array of BSON values from the provided {@code BsonReader},
     * constructing a list of {@code BqlValue} objects that represent the parsed elements.
     * <p>
     * The method expects the reader to be positioned at the start of an array, processes
     * each BSON element until reaching the end of the array, and converts each element
     * into its corresponding {@code BqlValue} representation.
     *
     * @param reader the {@code BsonReader} instance used to read the BSON array
     * @return a list of {@code BqlValue} objects parsed from the BSON array
     */
    private List<BqlValue> readValueArray(BsonReader reader) {
        reader.readStartArray();
        List<BqlValue> values = new ArrayList<>();

        while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
            values.add(readValue(reader));
        }

        reader.readEndArray();
        return values;
    }

    /**
     * Determines if a string represents a Base32Hex encoded Versionstamp.
     *
     * @param value the string to check
     * @return true if the string appears to be a Versionstamp encoding
     */
    private boolean isVersionstampString(String value) {
        // Versionstamps have a specific encoded length and character set
        return value != null &&
                value.length() == VersionstampUtil.EncodedVersionstampSize &&
                value.matches("[0-9A-V]+[x]*"); // Base32Hex characters + padding
    }

    /**
     * Reads a BSON value from the provided {@code BsonReader} and converts it into
     * an appropriate {@code BqlValue} representation based on the type of the BSON data.
     * <p>
     * The method supports various BSON types, including strings, integers, doubles, booleans,
     * documents, and arrays. Unsupported BSON types will result in a {@code BqlParseException}.
     *
     * @param reader the {@code BsonReader} instance used to read the BSON value
     * @return a {@code BqlValue} object representing the parsed BSON value
     * @throws BqlParseException if an unsupported BSON type is encountered
     */
    private BqlValue readValue(BsonReader reader) {
        return switch (reader.getCurrentBsonType()) {
            case STRING -> {
                String stringValue = reader.readString();
                // Check if the string represents a Versionstamp (Base32Hex encoded)
                if (isVersionstampString(stringValue)) {
                    try {
                        yield new VersionstampVal(VersionstampUtil.base32HexDecode(stringValue));
                    } catch (Exception e) {
                        // If decoding fails, treat as regular string
                        yield new StringVal(stringValue);
                    }
                }
                yield new StringVal(stringValue);
            }
            case INT32 -> new Int32Val(reader.readInt32());
            case INT64 -> new Int64Val(reader.readInt64());
            case DECIMAL128 -> new Decimal128Val(reader.readDecimal128().bigDecimalValue());
            case DOUBLE -> new DoubleVal(reader.readDouble());
            case BOOLEAN -> new BooleanVal(reader.readBoolean());
            case NULL -> {
                reader.readNull();
                yield NullVal.INSTANCE;
            }
            case BINARY -> new BinaryVal(reader.readBinaryData().getData());
            case DATE_TIME -> new DateTimeVal(reader.readDateTime());
            case TIMESTAMP -> new TimestampVal(reader.readTimestamp().getValue());
            case DOCUMENT -> {
                Map<String, BqlValue> fields = new LinkedHashMap<>();
                reader.readStartDocument();
                while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
                    String name = reader.readName();
                    fields.put(name, readValue(reader));
                }
                reader.readEndDocument();
                yield new DocumentVal(fields);
            }
            case ARRAY -> new ArrayVal(readValueArray(reader));
            default -> throw new BqlParseException("Unsupported value type: " + reader.getCurrentBsonType());
        };
    }
}
