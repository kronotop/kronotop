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

import com.kronotop.bucket.bql.ast.*;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Renders a BQL expression as text with every value replaced by {@code ?}.
 * The result shows the query shape without user data, like a parameterized SQL statement.
 */
public class QueryText {
    private static final String PARAM = "?";

    private QueryText() {
    }

    public static String render(BqlExpr expr) {
        return switch (expr) {
            case BqlAnd and -> "{\"$and\": [" + join(and.children()) + "]}";
            case BqlOr or -> "{\"$or\": [" + join(or.children()) + "]}";
            case BqlNot not -> "{\"$not\": " + render(not.expr()) + "}";
            case BqlElemMatch em -> field(em.selector(), "{\"$elemMatch\": " + render(em.expr()) + "}");
            case BqlEq eq -> field(eq.selector(), PARAM);
            case BqlNe ne -> operator(ne.selector(), "$ne");
            case BqlGt gt -> operator(gt.selector(), "$gt");
            case BqlGte gte -> operator(gte.selector(), "$gte");
            case BqlLt lt -> operator(lt.selector(), "$lt");
            case BqlLte lte -> operator(lte.selector(), "$lte");
            case BqlIn in -> operator(in.selector(), "$in");
            case BqlNin nin -> operator(nin.selector(), "$nin");
            case BqlAll all -> operator(all.selector(), "$all");
            case BqlExists exists -> operator(exists.selector(), "$exists");
            case BqlSize size -> operator(size.selector(), "$size");
            case BqlRegex regex -> operator(regex.selector(), "$regex");
        };
    }

    private static String join(List<BqlExpr> children) {
        return children.stream().map(QueryText::render).collect(Collectors.joining(", "));
    }

    private static String field(String selector, String value) {
        return "{\"" + selector + "\": " + value + "}";
    }

    private static String operator(String selector, String op) {
        return field(selector, "{\"" + op + "\": " + PARAM + "}");
    }
}
