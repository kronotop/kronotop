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

package com.kronotop.common.utils;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class DirectoryLayoutTest {
    @Test
    public void testDirectoryNamingTest() {
        String subspaceName = DirectoryLayout.Builder.clusterName("development").internal().redis().volume().dataStructure("string").toString();
        String expectedSubspaceName = "kronotop.development.internal.redis.syncer.0.string";
        assertEquals(expectedSubspaceName, subspaceName);
    }

    @Test
    public void testDirectoryNamingTest_addAll() {
        DirectoryLayout subspace = DirectoryLayout.Builder.clusterName("development").internal();
        List<String> path = Arrays.asList("foo", "bar");
        subspace.addAll(path);
        String expectedSubspaceName = "kronotop.development.internal.foo.bar";
        assertEquals(expectedSubspaceName, subspace.toString());
    }
}