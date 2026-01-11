/*
 * Copyright (c) 2023-2026 Burak Sezer
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.kronotop.bucket.pipeline;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.bucket.BSONUtil;
import com.kronotop.bucket.BucketMetadata;
import com.kronotop.internal.VersionstampUtil;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LtOperatorIntegrationTest extends BasePipelineTest {

    @Test
    void shouldReturnEmptyResultWhenQueryingIdLessThanMin() {
        final String TEST_BUCKET_NAME = "test-bucket-id-lt-min";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        List<byte[]> documents = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            documents.add(BSONUtil.jsonToDocumentThenBytes("{'status': 'ALIVE'}"));
        }

        List<Versionstamp> versionstamps = insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);
        Versionstamp smallestId = versionstamps.getFirst();

        String query = String.format("{'_id': {'$lt': '%s'}}", VersionstampUtil.base32HexEncode(smallestId));
        PipelineNode plan = createExecutionPlan(metadata, query);
        assertInstanceOf(IndexScanNode.class, plan);
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, config, plan);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);
            assertTrue(results.isEmpty());
        }
    }
}
