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

class Explain {
    static String explain(BqlExpr expr, int indent) {
        String pad = "  ".repeat(indent);
        switch (expr) {
            case BqlAnd and -> {
                String children = and.children().stream()
                        .map(e -> explain(e, indent + 1))
                        .reduce((a, b) -> a + "\n" + b).orElse("");
                return pad + "BqlAnd \n" + children;
            }
            case BqlOr or -> {
                String children = or.children().stream()
                        .map(e -> explain(e, indent + 1))
                        .reduce((a, b) -> a + "\n" + b).orElse("");
                return pad + "BqlOr \n" + children;
            }
            case BqlNot not -> {
                return pad + "BqlNot \n" + explain(not.expr(), indent + 1);
            }
            case BqlElemMatch em -> {
                return pad + "BqlElemMatch(selector=" + em.selector() + ")\n" + explain(em.expr(), indent + 1);
            }
            case BqlEq eq -> {
                return pad + "BqlEq(selector=" + eq.selector() + ", value=" + eq.value() + ")";
            }
            case BqlGt gt -> {
                return pad + "BqlGt(selector=" + gt.selector() + ", value=" + gt.value() + ")";
            }
            case BqlLt lt -> {
                return pad + "BqlLt(selector=" + lt.selector() + ", value=" + lt.value() + ")";
            }
            case BqlGte gte -> {
                return pad + "BqlGte(selector=" + gte.selector() + ", value=" + gte.value() + ")";
            }
            case BqlLte lte -> {
                return pad + "BqlLte(selector=" + lte.selector() + ", value=" + lte.value() + ")";
            }
            case BqlNe ne -> {
                return pad + "BqlNe(selector=" + ne.selector() + ", value=" + ne.value() + ")";
            }
            case BqlIn in -> {
                return pad + "BqlIn(selector=" + in.selector() + ", values=" + in.values() + ")";
            }
            case BqlNin nin -> {
                return pad + "BqlNin(selector=" + nin.selector() + ", values=" + nin.values() + ")";
            }
            case BqlExists exists -> {
                return pad + "BqlExists(selector=" + exists.selector() + ", exists=" + exists.exists() + ")";
            }
            case BqlAll all -> {
                return pad + "BqlAll(selector=" + all.selector() + ", values=" + all.values() + ")";
            }
            case BqlSize size -> {
                return pad + "BqlSize(selector=" + size.selector() + ", size=" + size.size() + ")";
            }
            default -> {
                return pad + expr;
            }
        }
    }
}
