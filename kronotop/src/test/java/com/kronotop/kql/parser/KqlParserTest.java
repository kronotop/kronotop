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

import com.kronotop.kql.KqlNode;
import org.junit.jupiter.api.Test;

public class KqlParserTest {
    @Test
    public void test_parser() {
        KqlNode parsed = KqlParser.parse("{ $or: [ { status: 'A' }, { qty: { $lt: 30 } } ] }");
        System.out.println(parsed);
    }
}
