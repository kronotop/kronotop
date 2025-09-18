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

package com.kronotop.bucket.statistics;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.kronotop.BaseStandaloneInstanceTest;
import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.BucketMetadataUtil;
import com.kronotop.bucket.index.IndexDefinition;
import com.kronotop.bucket.index.IndexUtil;
import com.kronotop.bucket.index.SortOrder;
import com.kronotop.server.Session;
import org.bson.BsonType;
import org.junit.jupiter.api.BeforeEach;

public class BasePrefixHistogramTest extends BaseStandaloneInstanceTest {
    protected PrefixHistogram histogram;

    protected void createIndex(String bucketName, IndexDefinition... definitions) {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            BucketMetadata metadata = getBucketMetadata(bucketName);
            for (IndexDefinition definition : definitions) {
                IndexUtil.create(tr, metadata.subspace(), definition);
            }
            tr.commit().join();
        }
    }

    protected BucketMetadata createIndexesAndLoadBucketMetadata(String bucketName, IndexDefinition definition) {
        // Create the bucket first
        Session session = getSession();
        BucketMetadataUtil.createOrOpen(context, session, bucketName);

        createIndex(bucketName, definition);

        // Load and return metadata
        return BucketMetadataUtil.createOrOpen(context, session, bucketName);
    }

    @BeforeEach
    void setUp() {
        IndexDefinition nameIndex = IndexDefinition.create("name-index", "name", BsonType.STRING, SortOrder.ASCENDING);
        BucketMetadata bucketMetadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, nameIndex);
        DirectorySubspace indexSubspace = bucketMetadata.indexes().getSubspace("name");

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            PrefixHistogram.initialize(tr, indexSubspace.getPath());
            tr.commit().join();
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            histogram = new PrefixHistogram(tr, indexSubspace.getPath());
            // Don't commit here - let individual tests commit
        }
    }
}