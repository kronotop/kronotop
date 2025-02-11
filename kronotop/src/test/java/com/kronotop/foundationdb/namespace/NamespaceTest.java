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

package com.kronotop.foundationdb.namespace;

import com.kronotop.BaseClusterTest;
import com.kronotop.Context;
import com.kronotop.KronotopTestInstance;
import com.kronotop.NamespaceUtils;
import com.kronotop.volume.Prefix;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class NamespaceTest extends BaseClusterTest {

    @Test
    void test_bucket_prefix_methods() {
        KronotopTestInstance instance = getInstances().getFirst();
        Context context = instance.getContext();

        String name = "one.two.three";
        Namespace namespace = NamespaceUtils.createOrOpen(context, name);

        String bucket = "bucket-name";
        Prefix prefix = new Prefix("prefix-name");

        assertDoesNotThrow(() -> namespace.setBucketPrefix(bucket, prefix));
        assertEquals(prefix, namespace.getBucketPrefix(bucket));
        assertNotNull(namespace.getBucketIndexSubspace(1, prefix));
        assertNotNull(namespace.getBucketPrefixesSubspace());
    }

    @Test
    void test_cleanupBucket() {
        KronotopTestInstance instance = getInstances().getFirst();
        Context context = instance.getContext();

        String name = "one.two.three";
        Namespace namespace = NamespaceUtils.createOrOpen(context, name);

        String bucket = "bucket-name";
        Prefix prefix = new Prefix("prefix-name");

        assertDoesNotThrow(() -> namespace.setBucketPrefix(bucket, prefix));
        assertDoesNotThrow(() -> namespace.cleanupBucket(bucket));
    }
}