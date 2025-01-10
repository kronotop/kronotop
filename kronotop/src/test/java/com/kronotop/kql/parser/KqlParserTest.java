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

import com.kronotop.kql.operators.KqlOperator;
import org.junit.jupiter.api.Test;

import java.util.List;

/*
{
    "$or": [
        {
            "status": {
                "$eq": "A"
            }
        },
        {
            "qty": {
                "$lt": 30
            }
        }
    ]
}
 */

/*
{
    "status": "A",
    "username:": "buraksezer"
}

         KqlNode
        /       \
       /         \
      /           \
   KqlNode       KqlNode
(KqlEqOperator)  (KqlEqOperator)
 */

class KqlParserTest {
    @Test
    void test_parser() {
        // {}
        //KqlNode parsed = KqlParser.parse("{ }");
        // { status: 'A' }
        // { status: 'B', username: 'buraksezer' }
        //List<KqlOperator> parsed = KqlParser.parse("{ status: 'B', username: 'buraksezer' }");
        //List<KqlOperator> parsed = KqlParser.parse("{ $or: [ { status: {$eq: 'A' } }, { qty: { $lt: 30 } } ], username: { $eq: 'buraksezer' } }");
        //KqlNode parsed = KqlParser.parse("{ status: 'A' }");
        //List<KqlOperator> parsed = KqlParser.parse("{ $and: [ { scores: 75, name: 'Greg Powell' } ] }");
        //List<KqlOperator> parsed = KqlParser.parse("{ height: { $gt: 8500 } }");
        //List<KqlOperator> parsed = KqlParser.parse("{ tags: { $all: [ 'ssl' , 'security' ] } }");
        //List<KqlOperator> parsed = KqlParser.parse("{ height: { $gt: 8500 } }");
        List<KqlOperator> parsed = KqlParser.parse("{ quantity: { $nin: [ 5, 15 ] } }");
        for (KqlOperator kqlOperator : parsed) {
            System.out.println(kqlOperator);
        }
    }
}
