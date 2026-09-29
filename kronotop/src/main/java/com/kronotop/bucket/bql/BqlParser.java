/*
 * Copyright (c) 2023-2026 Burak Sezer
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

import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.bucket.bql.ast.*;
import com.kronotop.internal.VersionstampUtil;
import org.bson.*;
import org.bson.types.ObjectId;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Parses Bucket Query Language (BQL) into a {@code BqlExpr} tree.
 * <p>
 * Input can be a BQL string, a BSON byte array, or a BSON document. The parser covers
 * field selectors and comparison, array, and logical operators.
 * <p>
 * For scalar arrays such as {@code tags: ["urgent", "bug"]}, operators inside {@code $elemMatch}
 * use an empty string {@code ""} as the selector, because scalar elements are wrapped in
 * {@code {"": value}} during evaluation. The empty selector stands for "the element itself"
 * and is never a user-visible field name. For example, {@code { tags: { $elemMatch: { $eq: "urgent" } } }}
 * parses with selector {@code ""}.
 */
public class BqlParser {
    private static final int MINIMUM_BSON_DOCUMENT_SIZE = 5;
    private static final byte BSON_TERMINATOR = 0x00;

    public static String explain(BqlExpr expr) {
        return Explain.explain(expr, 0);
    }

    /**
     * Parses a BQL query string.
     *
     * @param query the BQL query string
     * @return the parsed expression
     * @throws BqlParseException if the query is malformed or uses an unsupported operator
     */
    public static BqlExpr parse(String query) {
        BsonDocument document;
        try {
            document = BsonDocument.parse(query);
        } catch (Exception e) {
            throw new BqlParseException("Invalid BSON format", e);
        }

        return parse(document);
    }

    /**
     * Determines if the given byte array is a valid BSON document by checking
     * the BSON structural invariants: minimum size, null terminator, and
     * declared length matching actual length.
     */
    public static boolean isBSON(byte[] query) {
        if (query.length < MINIMUM_BSON_DOCUMENT_SIZE) {
            return false;
        }
        if (query[query.length - 1] != BSON_TERMINATOR) {
            return false;
        }
        int declaredLength = (query[0] & 0xFF)
                | ((query[1] & 0xFF) << 8)
                | ((query[2] & 0xFF) << 16)
                | ((query[3] & 0xFF) << 24);
        return declaredLength == query.length;
    }

    /**
     * Parses a BSON-encoded query.
     *
     * @param query the BSON-encoded query
     * @return the parsed expression
     * @throws BqlParseException if the BSON is invalid or the query cannot be parsed
     */
    public static BqlExpr parse(byte[] query) {
        if (!isBSON(query)) {
            return parse(new String(query));
        }

        try (BsonReader reader = new BsonBinaryReader(ByteBuffer.wrap(query))) {
            return new BqlParser().parse(reader);
        } catch (BqlParseException e) {
            throw e;
        } catch (Exception e) {
            throw new BqlParseException("Invalid BSON format", e);
        }
    }

    /**
     * Parses a BSON document.
     *
     * @param document the query document, not null
     * @return the parsed expression
     * @throws BqlParseException if the document cannot be parsed
     */
    private static BqlExpr parse(BsonDocument document) {
        try (BsonReader reader = document.asBsonReader()) {
            return new BqlParser().parse(reader);
        } catch (BqlParseException e) {
            // Re-throw our own exceptions as-is
            throw e;
        } catch (Exception e) {
            // Only catch BSON processing errors and other non-BQL exceptions
            throw new BqlParseException("BSON processing error", e);
        }
    }

    /**
     * Builds a {@code BqlRegex} for the given selector from a native BSON regular expression value,
     * carrying its pattern and options.
     */
    private static BqlRegex nativeRegex(String selector, BsonRegularExpression regex) {
        return new BqlRegex(selector, new RegexVal(regex.getPattern(), regex.getOptions()));
    }

    private BqlExpr parse(BsonReader reader) {
        return readExpr(reader);
    }

    /**
     * Reads a query document. Multiple top-level expressions are combined into a {@code BqlAnd}.
     *
     * @param reader the reader, positioned at the document
     * @return the parsed expression
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
     * Parses one key-value pair. A key that starts with {@code $} is a logical operator,
     * any other key is a selector.
     *
     * @param reader the reader, positioned at the value
     * @param key    the operator or selector name
     * @return the parsed expression
     * @throws BqlParseException if the operator is unknown or the value type is not supported
     */
    private BqlExpr parseSelectorOrOperator(BsonReader reader, String key) {
        if (key.startsWith("$")) {
            return switch (key) {
                case "$and" -> new BqlAnd(readArray(reader));
                case "$or" -> new BqlOr(readArray(reader));
                case "$nor" -> new BqlNot(new BqlOr(readArray(reader)));
                case "$not" -> new BqlNot(readExpr(reader));
                default -> throw new BqlParseException("Unknown operator: " + key);
            };
        } else {
            // It's a selector name
            BsonType type = reader.getCurrentBsonType();
            if (type == BsonType.DOCUMENT) {
                return readSelectorExpression(reader, key);
            }
            if (type == BsonType.REGULAR_EXPRESSION) {
                return nativeRegex(key, reader.readRegularExpression());
            }
            return new BqlEq(key, readValue(reader));
        }
    }

    /**
     * Same as {@link #parseSelectorOrOperator}, but inside {@code $elemMatch}.
     *
     * @param reader            the reader, positioned at the value
     * @param key               the operator or selector name
     * @param elemMatchSelector the selector that owns the {@code $elemMatch}
     * @return the parsed expression
     */
    private BqlExpr parseSelectorOrOperatorInElemMatch(BsonReader reader, String key, String elemMatchSelector) {
        if (key.startsWith("$")) {
            return switch (key) {
                case "$and" -> new BqlAnd(readArrayInElemMatch(reader, elemMatchSelector));
                case "$or" -> new BqlOr(readArrayInElemMatch(reader, elemMatchSelector));
                case "$nor" -> new BqlNot(new BqlOr(readArrayInElemMatch(reader, elemMatchSelector)));
                case "$not" -> new BqlNot(readElemMatchExpr(reader, elemMatchSelector));
                // For scalar array $elemMatch (e.g., {'tags': {'$elemMatch': {'$eq': 'urgent'}}}),
                // use empty selector "" because scalar elements are wrapped in {"": value}
                // This is a semantic invariant of scalar elemMatch.
                default -> parseSelectorOperator(reader, key, "");
            };
        } else {
            // It's a selector name
            BsonType type = reader.getCurrentBsonType();
            if (type == BsonType.DOCUMENT) {
                return readSelectorExpression(reader, key);
            }
            if (type == BsonType.REGULAR_EXPRESSION) {
                return nativeRegex(key, reader.readRegularExpression());
            }
            return new BqlEq(key, readValue(reader));
        }
    }

    /**
     * Reads an array of expressions within $elemMatch context.
     * Each element is parsed with $elemMatch semantics (empty selector for scalar operators).
     */
    private List<BqlExpr> readArrayInElemMatch(BsonReader reader, String elemMatchSelector) {
        reader.readStartArray();
        List<BqlExpr> list = new ArrayList<>();

        while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
            list.add(readElemMatchExpr(reader, elemMatchSelector));
        }

        reader.readEndArray();
        return list;
    }

    /**
     * Parses a selector operator and its value into a {@code BqlExpr}. Supported operators are
     * the comparisons ({@code $gt}, {@code $lt}, {@code $gte}, {@code $lte}, {@code $eq},
     * {@code $ne}), the array operators ({@code $in}, {@code $nin}, {@code $all}), and
     * {@code $size} and {@code $exists}.
     *
     * @param reader   the reader, positioned at the operator value
     * @param op       the operator, e.g. {@code $gt}
     * @param selector the selector the operator applies to
     * @return the parsed expression
     * @throws BqlParseException if the operator is unknown, or if the value type is invalid for the operator
     */
    private BqlExpr parseSelectorOperator(BsonReader reader, String op, String selector) {
        return switch (op) {
            case "$gt" -> new BqlGt(selector, readNonNullValue(reader, op));
            case "$lt" -> new BqlLt(selector, readNonNullValue(reader, op));
            case "$gte" -> new BqlGte(selector, readNonNullValue(reader, op));
            case "$lte" -> new BqlLte(selector, readNonNullValue(reader, op));
            case "$eq" -> new BqlEq(selector, readValue(reader));
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
     * Reads a value that must not be null. Throws BqlParseException if null is encountered.
     */
    private BqlValue readNonNullValue(BsonReader reader, String op) {
        if (reader.getCurrentBsonType() == BsonType.NULL) {
            throw new BqlParseException(op + " operator does not support null values");
        }
        return readValue(reader);
    }

    /**
     * Reads the operator document of a selector, such as {@code {$gt: 1, $lt: 5}}. Multiple operators
     * are combined with an implicit {@code $and}.
     *
     * @param reader   the reader, positioned at the operator document
     * @param selector the selector the operators apply to
     * @return the parsed expression
     * @throws BqlParseException if the selector operator document is empty or invalid
     */
    private BqlExpr readSelectorExpression(BsonReader reader, String selector) {
        reader.readStartDocument();

        if (reader.readBsonType() == BsonType.END_OF_DOCUMENT) {
            reader.readEndDocument();
            throw new BqlParseException("Empty selector operator document for: " + selector);
        }

        List<BqlExpr> expressions = new ArrayList<>();

        // $regex and $options are sibling fields in the same selector document, so they are
        // buffered here and combined into a single BqlRegex after the document is fully read.
        String regexPattern = null;
        String regexOptions = null;
        boolean hasOptions = false;

        // Process all operators in the selector document
        do {
            String op = reader.readName();
            switch (op) {
                case "$regex" -> {
                    BsonType regexType = reader.getCurrentBsonType();
                    if (regexType == BsonType.STRING) {
                        regexPattern = reader.readString();
                    } else if (regexType == BsonType.REGULAR_EXPRESSION) {
                        // A regular expression value carries the pattern and, optionally, its own options.
                        // An explicit $options sibling, if present, takes precedence.
                        BsonRegularExpression regex = reader.readRegularExpression();
                        regexPattern = regex.getPattern();
                        if (regexOptions == null && !regex.getOptions().isEmpty()) {
                            regexOptions = regex.getOptions();
                        }
                    } else {
                        throw new BqlParseException("$regex expects a string pattern or a regular expression");
                    }
                }
                case "$options" -> {
                    if (reader.getCurrentBsonType() != BsonType.STRING) {
                        throw new BqlParseException("$options expects a string");
                    }
                    regexOptions = reader.readString();
                    hasOptions = true;
                }
                case "$elemMatch" -> expressions.add(new BqlElemMatch(selector, readElemMatchExpr(reader, selector)));
                case "$not" -> {
                    // $not may negate an operator document ({$gt: 5}) or a regex value directly ({$not: /foo/}).
                    if (reader.getCurrentBsonType() == BsonType.REGULAR_EXPRESSION) {
                        expressions.add(new BqlNot(nativeRegex(selector, reader.readRegularExpression())));
                    } else {
                        expressions.add(new BqlNot(readSelectorExpression(reader, selector)));
                    }
                }
                default -> expressions.add(parseSelectorOperator(reader, op, selector));
            }
        } while (reader.readBsonType() != BsonType.END_OF_DOCUMENT);

        reader.readEndDocument();

        if (regexPattern != null) {
            expressions.add(new BqlRegex(selector, new RegexVal(regexPattern, regexOptions)));
        } else if (hasOptions) {
            throw new BqlParseException("$options requires $regex");
        }

        // If only one expression, return it directly
        if (expressions.size() == 1) {
            return expressions.getFirst();
        }

        // Multiple expressions are combined with implicit AND
        return new BqlAnd(expressions);
    }

    /**
     * Reads the body of an {@code $elemMatch}. Multiple expressions are combined into a {@code BqlAnd}.
     *
     * @param reader            the reader, positioned at the document
     * @param elemMatchSelector the selector that owns the {@code $elemMatch}
     * @return the parsed expression
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
     * Reads an array of expressions, such as the operands of {@code $and}.
     *
     * @param reader the reader, positioned at the array
     * @return the parsed expressions
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
     * Reads an array of values, such as the operand of {@code $in}.
     *
     * @param reader the reader, positioned at the array
     * @return the parsed values
     */
    private List<BqlValue> readValueArray(BsonReader reader) {
        reader.readStartArray();
        List<BqlValue> values = new ArrayList<>();

        while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
            // A regular expression literal is a valid array element only here, where the array feeds
            // $in, $nin, and $all. Each such element acts as a regex matcher, not a stored value.
            if (reader.getCurrentBsonType() == BsonType.REGULAR_EXPRESSION) {
                BsonRegularExpression regex = reader.readRegularExpression();
                values.add(new RegexVal(regex.getPattern(), regex.getOptions()));
            } else {
                values.add(readValue(reader));
            }
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
        // Versionstamps have a specific encoded length
        return value != null &&
                value.length() == VersionstampUtil.EncodedVersionstampSize;
    }

    /**
     * Reads a BSON value as a {@code BqlValue}.
     *
     * @param reader the reader, positioned at the value
     * @return the parsed value
     * @throws BqlParseException if an unsupported BSON type is encountered
     */
    private BqlValue readValue(BsonReader reader) {
        return switch (reader.getCurrentBsonType()) {
            case STRING -> {
                String stringValue = reader.readString();
                // First, check for the ObjectId
                if (ObjectId.isValid(stringValue)) {
                    try {
                        yield new ObjectIdVal(new ObjectId(stringValue));
                    } catch (Exception ignored) {
                        // fall through as string
                    }
                }

                // Best-effort interpretation for VersionstampVal:
                //  - Check if the string represents a Versionstamp (Base32Hex encoded)
                if (isVersionstampString(stringValue)) {
                    try {
                        Versionstamp versionstamp = VersionstampUtil.base32HexDecode(stringValue);
                        if (versionstamp.isComplete()) {
                            yield new VersionstampVal(versionstamp);
                        }
                        // incomplete versionstamp: fall through as string
                    } catch (Exception e) {
                        // If decoding fails, treat as regular StringVal
                        yield new StringVal(stringValue);
                    }
                }
                yield new StringVal(stringValue);
            }
            case OBJECT_ID -> new ObjectIdVal(reader.readObjectId());
            case INT32 -> new Int32Val(reader.readInt32());
            case INT64 -> new Int64Val(reader.readInt64());
            case DECIMAL128 -> new Decimal128Val(reader.readDecimal128().bigDecimalValue());
            case DOUBLE -> new DoubleVal(reader.readDouble());
            case BOOLEAN -> new BooleanVal(reader.readBoolean());
            case NULL -> {
                reader.readNull();
                yield NullVal.INSTANCE;
            }
            case BINARY -> {
                byte[] data = reader.readBinaryData().getData();
                // Best-effort interpretation: 12 bytes (10 transaction + 2 user) binaries may represent versionstamps.
                // Incomplete or invalid candidates are treated as opaque binary values.
                if (data.length == Versionstamp.LENGTH) {
                    try {
                        Versionstamp versionstamp = Versionstamp.fromBytes(data);
                        if (versionstamp.isComplete()) {
                            yield new VersionstampVal(versionstamp);
                        }
                        // incomplete versionstamp: fall through as binary
                    } catch (Exception e) {
                        // If decoding fails, treat as regular BinaryVal
                        yield new BinaryVal(data);
                    }
                }
                yield new BinaryVal(data);
            }
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
