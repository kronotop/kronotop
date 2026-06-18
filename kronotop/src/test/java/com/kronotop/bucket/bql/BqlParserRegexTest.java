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

import com.kronotop.bucket.BSONUtil;
import com.kronotop.bucket.bql.ast.BqlAll;
import com.kronotop.bucket.bql.ast.BqlAnd;
import com.kronotop.bucket.bql.ast.BqlElemMatch;
import com.kronotop.bucket.bql.ast.BqlExists;
import com.kronotop.bucket.bql.ast.BqlExpr;
import com.kronotop.bucket.bql.ast.BqlIn;
import com.kronotop.bucket.bql.ast.BqlNin;
import com.kronotop.bucket.bql.ast.BqlNot;
import com.kronotop.bucket.bql.ast.BqlRegex;
import com.kronotop.bucket.bql.ast.RegexVal;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonRegularExpression;
import org.bson.BsonString;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class BqlParserRegexTest {

    @Test
    void shouldParseRegexWithOptions() {
        // Behavior: {field: {$regex: "...", $options: "..."}} parses to a BqlRegex carrying pattern and options.
        BqlExpr result = BqlParser.parse("{\"name\": {\"$regex\": \"^foo\", \"$options\": \"i\"}}");

        assertInstanceOf(BqlRegex.class, result);
        BqlRegex regex = (BqlRegex) result;
        assertEquals("name", regex.selector());
        assertEquals("^foo", regex.value().pattern());
        assertEquals("i", regex.value().options());
    }

    @Test
    void shouldParseRegexWithoutOptions() {
        // Behavior: $regex without $options parses with empty options.
        BqlExpr result = BqlParser.parse("{\"name\": {\"$regex\": \"^foo\"}}");

        assertInstanceOf(BqlRegex.class, result);
        BqlRegex regex = (BqlRegex) result;
        assertEquals("name", regex.selector());
        assertEquals("^foo", regex.value().pattern());
        assertEquals("", regex.value().options());
    }

    @Test
    void shouldParseRegexFromRawBsonDocumentForm() {
        // Behavior: a raw BSON document with literal $regex/$options string fields parses to a BqlRegex.
        BsonDocument inner = new BsonDocument();
        inner.put("$regex", new BsonString("^foo"));
        inner.put("$options", new BsonString("im"));
        BsonDocument doc = new BsonDocument("name", inner);

        BqlExpr result = BqlParser.parse(BSONUtil.toBytes(doc));

        assertInstanceOf(BqlRegex.class, result);
        BqlRegex regex = (BqlRegex) result;
        assertEquals("name", regex.selector());
        assertEquals("^foo", regex.value().pattern());
        assertEquals("im", regex.value().options());
    }

    @Test
    void shouldParseNotWithRegex() {
        // Behavior: $not negating a regex parses to a BqlNot wrapping a BqlRegex.
        BqlExpr result = BqlParser.parse("{\"name\": {\"$not\": {\"$regex\": \"^foo\"}}}");

        assertInstanceOf(BqlNot.class, result);
        BqlNot not = (BqlNot) result;
        assertInstanceOf(BqlRegex.class, not.expr());
        assertEquals("^foo", ((BqlRegex) not.expr()).value().pattern());
    }

    @Test
    void shouldAcceptAndIgnoreUnicodeOption() {
        // Behavior: the u option is accepted but redundant (RE2 is already UTF-8), so parsing succeeds.
        BqlExpr result = BqlParser.parse("{\"name\": {\"$regex\": \"^foo\", \"$options\": \"iu\"}}");

        assertInstanceOf(BqlRegex.class, result);
        BqlRegex regex = (BqlRegex) result;
        assertEquals("^foo", regex.value().pattern());
        assertEquals("iu", regex.value().options());
    }

    @Test
    void shouldParseNativeRegexAsRegexOperatorValue() {
        // Behavior: a $regex value that is itself a regular expression carries the pattern and its options.
        BsonDocument inner = new BsonDocument();
        inner.put("$regex", new org.bson.BsonRegularExpression("^foo", "i"));
        BsonDocument doc = new BsonDocument("name", inner);

        BqlExpr result = BqlParser.parse(BSONUtil.toBytes(doc));

        assertInstanceOf(BqlRegex.class, result);
        BqlRegex regex = (BqlRegex) result;
        assertEquals("name", regex.selector());
        assertEquals("^foo", regex.value().pattern());
        assertEquals("i", regex.value().options());
    }

    @Test
    void shouldLetExplicitOptionsOverrideNativeRegexOptions() {
        // Behavior: an explicit $options sibling takes precedence over options carried by the regex value.
        BsonDocument inner = new BsonDocument();
        inner.put("$regex", new org.bson.BsonRegularExpression("^foo", "i"));
        inner.put("$options", new BsonString("m"));
        BsonDocument doc = new BsonDocument("name", inner);

        BqlExpr result = BqlParser.parse(BSONUtil.toBytes(doc));

        assertInstanceOf(BqlRegex.class, result);
        assertEquals("m", ((BqlRegex) result).value().options());
    }

    @Test
    void shouldExplainRegex() {
        // Behavior: the explain output of a $regex query names the BqlRegex node with its selector and value.
        BqlExpr result = BqlParser.parse("{\"name\": {\"$regex\": \"^foo\", \"$options\": \"i\"}}");

        String explanation = BqlParser.explain(result);
        assertTrue(explanation.contains("BqlRegex"), "Explain should name the BqlRegex node");
        assertTrue(explanation.contains("name"), "Explain should contain the selector");
        assertTrue(explanation.contains("^foo"), "Explain should contain the pattern");
    }

    @Test
    void shouldRejectUnsupportedRegexOption() {
        // Behavior: an unsupported option (x) is rejected at parse time.
        assertThrows(BqlParseException.class, () ->
                BqlParser.parse("{\"name\": {\"$regex\": \"^foo\", \"$options\": \"x\"}}"));
    }

    @Test
    void shouldRejectInvalidRegexPattern() {
        // Behavior: a malformed pattern is rejected at parse time.
        assertThrows(BqlParseException.class, () ->
                BqlParser.parse("{\"name\": {\"$regex\": \"(unclosed\"}}"));
    }

    @Test
    void shouldRejectBackreferencePattern() {
        // Behavior: a backreference pattern is rejected at parse time because RE2 does not support it.
        assertThrows(BqlParseException.class, () ->
                BqlParser.parse("{\"name\": {\"$regex\": \"(a)\\\\1\"}}"));
    }

    @Test
    void shouldRejectLookaroundPattern() {
        // Behavior: a lookahead pattern is rejected at parse time because RE2 does not support it.
        assertThrows(BqlParseException.class, () ->
                BqlParser.parse("{\"name\": {\"$regex\": \"foo(?=bar)\"}}"));
    }

    @Test
    void shouldRejectOptionsWithoutRegex() {
        // Behavior: $options without a $regex sibling is rejected.
        BsonDocument inner = new BsonDocument();
        inner.put("$options", new BsonString("i"));
        BsonDocument doc = new BsonDocument("name", inner);

        assertThrows(BqlParseException.class, () -> BqlParser.parse(BSONUtil.toBytes(doc)));
    }

    @Test
    void shouldParseBareNativeRegexAtTopLevel() {
        // Behavior: a bare regular expression as a field value parses to a BqlRegex.
        BsonDocument doc = new BsonDocument("name", new BsonRegularExpression("^foo", "i"));

        BqlExpr result = BqlParser.parse(BSONUtil.toBytes(doc));

        assertInstanceOf(BqlRegex.class, result);
        BqlRegex regex = (BqlRegex) result;
        assertEquals("name", regex.selector());
        assertEquals("^foo", regex.value().pattern());
        assertEquals("i", regex.value().options());
    }

    @Test
    void shouldParseBareNativeRegexInsideElemMatch() {
        // Behavior: a bare regular expression on a field inside $elemMatch parses to a BqlRegex.
        BsonDocument inner = new BsonDocument("tag", new BsonRegularExpression("^foo", ""));
        BsonDocument elem = new BsonDocument("$elemMatch", inner);
        BsonDocument doc = new BsonDocument("items", elem);

        BqlExpr result = BqlParser.parse(BSONUtil.toBytes(doc));

        assertInstanceOf(BqlElemMatch.class, result);
        BqlElemMatch elemMatch = (BqlElemMatch) result;
        assertEquals("items", elemMatch.selector());
        assertInstanceOf(BqlRegex.class, elemMatch.expr());
        BqlRegex regex = (BqlRegex) elemMatch.expr();
        assertEquals("tag", regex.selector());
        assertEquals("^foo", regex.value().pattern());
    }

    @Test
    void shouldRejectRegexWithNonStringNonRegexValue() {
        // Behavior: a $regex value that is neither a string nor a regular expression is rejected.
        BsonDocument inner = new BsonDocument("$regex", new BsonInt32(123));
        BsonDocument doc = new BsonDocument("name", inner);

        assertThrows(BqlParseException.class, () -> BqlParser.parse(BSONUtil.toBytes(doc)));
    }

    @Test
    void shouldRejectOptionsWithNonStringValue() {
        // Behavior: a non-string $options value is rejected.
        BsonDocument inner = new BsonDocument();
        inner.put("$regex", new BsonString("^foo"));
        inner.put("$options", new BsonInt32(1));
        BsonDocument doc = new BsonDocument("name", inner);

        assertThrows(BqlParseException.class, () -> BqlParser.parse(BSONUtil.toBytes(doc)));
    }

    @Test
    void shouldCombineRegexWithSiblingOperatorAsAnd() {
        // Behavior: $regex alongside another operator in the same selector document yields an AND.
        BqlExpr result = BqlParser.parse("{\"name\": {\"$regex\": \"^a\", \"$exists\": true}}");

        assertInstanceOf(BqlAnd.class, result);
        BqlAnd and = (BqlAnd) result;
        assertEquals(2, and.children().size());
        assertTrue(and.children().stream().anyMatch(BqlExists.class::isInstance),
                "AND should contain the $exists expression");
        assertTrue(and.children().stream().anyMatch(BqlRegex.class::isInstance),
                "AND should contain the $regex expression");
    }

    @Test
    void shouldParseRegexLiteralsInsideIn() {
        // Behavior: regex literals as $in array elements parse to a BqlIn carrying RegexVal values.
        BsonDocument inner = new BsonDocument("$in", new BsonArray(List.of(
                new BsonRegularExpression("^a", "i"),
                new BsonRegularExpression("^b", ""))));
        BsonDocument doc = new BsonDocument("name", inner);

        BqlExpr result = BqlParser.parse(BSONUtil.toBytes(doc));

        assertInstanceOf(BqlIn.class, result);
        BqlIn in = (BqlIn) result;
        assertEquals("name", in.selector());
        assertEquals(2, in.values().size());
        assertInstanceOf(RegexVal.class, in.values().get(0));
        assertEquals("^a", ((RegexVal) in.values().get(0)).pattern());
        assertEquals("i", ((RegexVal) in.values().get(0)).options());
        assertInstanceOf(RegexVal.class, in.values().get(1));
    }

    @Test
    void shouldParseRegexLiteralsInsideNin() {
        // Behavior: regex literals as $nin array elements parse to a BqlNin carrying RegexVal values.
        BsonDocument inner = new BsonDocument("$nin",
                new BsonArray(List.of(new BsonRegularExpression("^a", ""))));
        BsonDocument doc = new BsonDocument("name", inner);

        BqlExpr result = BqlParser.parse(BSONUtil.toBytes(doc));

        assertInstanceOf(BqlNin.class, result);
        BqlNin nin = (BqlNin) result;
        assertEquals(1, nin.values().size());
        assertInstanceOf(RegexVal.class, nin.values().getFirst());
    }

    @Test
    void shouldParseRegexLiteralsInsideAll() {
        // Behavior: regex literals as $all array elements parse to a BqlAll carrying RegexVal values.
        BsonDocument inner = new BsonDocument("$all", new BsonArray(List.of(
                new BsonRegularExpression("^a", ""),
                new BsonRegularExpression("^b", ""))));
        BsonDocument doc = new BsonDocument("tags", inner);

        BqlExpr result = BqlParser.parse(BSONUtil.toBytes(doc));

        assertInstanceOf(BqlAll.class, result);
        BqlAll all = (BqlAll) result;
        assertEquals(2, all.values().size());
        assertInstanceOf(RegexVal.class, all.values().get(0));
        assertInstanceOf(RegexVal.class, all.values().get(1));
    }

    @Test
    void shouldParseMixedStringAndRegexLiteralsInsideIn() {
        // Behavior: $in may mix plain string values with regex literals.
        BsonDocument inner = new BsonDocument("$in", new BsonArray(List.of(
                new BsonString("exact"),
                new BsonRegularExpression("^a", ""))));
        BsonDocument doc = new BsonDocument("name", inner);

        BqlExpr result = BqlParser.parse(BSONUtil.toBytes(doc));

        assertInstanceOf(BqlIn.class, result);
        BqlIn in = (BqlIn) result;
        assertEquals(2, in.values().size());
        assertInstanceOf(RegexVal.class, in.values().get(1));
    }

    @Test
    void shouldRejectRegexLiteralAsEqValue() {
        // Behavior: a regex literal is not a stored value, so $eq with a regex literal is rejected.
        BsonDocument inner = new BsonDocument("$eq", new BsonRegularExpression("^a", ""));
        BsonDocument doc = new BsonDocument("name", inner);

        assertThrows(BqlParseException.class, () -> BqlParser.parse(BSONUtil.toBytes(doc)));
    }
}
