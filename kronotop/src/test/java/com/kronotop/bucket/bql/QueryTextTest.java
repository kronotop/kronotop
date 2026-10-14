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

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;

class QueryTextTest {

    @ParameterizedTest
    @CsvSource(delimiter = '|', value = {
            "{\"age\": {\"$gt\": 18}}                                  | {\"age\": {\"$gt\": ?}}",
            "{\"name\": \"Alice\"}                                     | {\"name\": ?}",
            "{\"tags\": {\"$in\": [\"a\", \"b\"]}}                     | {\"tags\": {\"$in\": ?}}",
            "{\"f\": {\"$exists\": true}}                              | {\"f\": {\"$exists\": ?}}",
            "{\"items\": {\"$size\": 3}}                               | {\"items\": {\"$size\": ?}}",
            "{\"name\": {\"$regex\": \"^Al\"}}                         | {\"name\": {\"$regex\": ?}}",
            "{\"$and\": [{\"a\": 1}, {\"b\": {\"$lt\": 2}}]}           | {\"$and\": [{\"a\": ?}, {\"b\": {\"$lt\": ?}}]}",
            "{\"$or\": [{\"a\": 1}, {\"b\": 2}]}                       | {\"$or\": [{\"a\": ?}, {\"b\": ?}]}",
            "{\"a\": {\"$not\": {\"$gte\": 5}}}                        | {\"$not\": {\"a\": {\"$gte\": ?}}}",
            "{\"items\": {\"$elemMatch\": {\"qty\": {\"$gt\": 5}}}}    | {\"items\": {\"$elemMatch\": {\"qty\": {\"$gt\": ?}}}}",
    })
    void shouldReplaceValuesWithPlaceholder(String query, String expected) {
        String rendered = QueryText.render(BqlParser.parse(query.trim().getBytes(StandardCharsets.UTF_8)));
        assertEquals(expected.trim(), rendered);
    }
}
