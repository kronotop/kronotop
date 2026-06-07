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

package com.kronotop.bucket.handlers;

import com.apple.foundationdb.Transaction;
import com.kronotop.KronotopException;
import com.kronotop.bucket.*;
import com.kronotop.bucket.bql.BqlParser;
import com.kronotop.bucket.bql.ast.BqlExpr;
import com.kronotop.bucket.handlers.protocol.BucketVectorMessage;
import com.kronotop.bucket.index.IndexSelectionPolicy;
import com.kronotop.bucket.index.VectorIndex;
import com.kronotop.bucket.pipeline.*;
import com.kronotop.bucket.vector.*;
import com.kronotop.internal.JSONUtil;
import com.kronotop.server.*;
import com.kronotop.server.annotation.Command;
import com.kronotop.server.annotation.MinimumParameterCount;
import com.kronotop.transaction.TransactionUtil;
import org.bson.BsonDocument;
import org.bson.types.ObjectId;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.kronotop.AsyncCommandExecutor.supplyAsync;

@Command(BucketVectorMessage.COMMAND)
@MinimumParameterCount(BucketVectorMessage.MINIMUM_PARAMETER_COUNT)
public class BucketVectorHandler extends AbstractBucketHandler implements Handler {
    private static final byte OPENING_BRACKET_ASCII_CODE = 0x5B; // '['
    private static final byte CLOSING_BRACKET_ASCII_CODE = 0x5D; // ']'
    private static final int DEFAULT_TOPK = 10;

    private final boolean planCacheEnabled;
    private final int planCacheMaxTtl;

    public BucketVectorHandler(BucketService service) {
        super(service);
        planCacheEnabled = context.getConfig().getBoolean("bucket.plan_cache.enabled");
        planCacheMaxTtl = context.getConfig().getInt("bucket.plan_cache.max_ttl");
    }

    @Override
    public void beforeExecute(Request request) {
        request.attr(MessageTypes.BUCKETVECTOR).set(new BucketVectorMessage(request));
    }

    private void validateVector(VectorIndex index, float[] floats) {
        if (floats.length != index.definition().dimensions()) {
            throw new KronotopException(
                    "Vector index '" + index.definition().name() + "' requires "
                            + index.definition().dimensions() + " dimensions but query vector has "
                            + floats.length
            );
        }

        for (int i = 0; i < floats.length; i++) {
            float f = floats[i];
            if (Float.isInfinite(f) || Float.isNaN(f)) {
                throw new IllegalCommandArgumentException("Vector component at index " + i + " overflows float range: " + f);
            }
        }
    }

    private float[] parseVector(byte[] data, int dimensions) {
        if (data.length == 0) {
            throw new IllegalCommandArgumentException("Vector cannot be empty");
        }

        // Binary vectors are always exactly dimensions * 4 bytes (little-endian float[]).
        // If the length doesn't match, the data cannot be a valid binary vector - try JSON.
        // The bracket check guards against accidentally parsing malformed binary as JSON.
        if (data.length != dimensions * 4
                && data[0] == OPENING_BRACKET_ASCII_CODE
                && data[data.length - 1] == CLOSING_BRACKET_ASCII_CODE) {
            // Intended for development purposes
            return JSONUtil.readValue(data, float[].class);
        }

        ByteBuffer buf = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);
        float[] floats = new float[data.length / 4];
        buf.asFloatBuffer().get(floats);
        return floats;
    }

    @Override
    public void execute(Request request, Response response) throws Exception {
        supplyAsync(context, response, () -> {
            BucketVectorMessage message = request.attr(MessageTypes.BUCKETVECTOR).get();
            Session session = request.getSession();

            try (Transaction tr = TransactionUtil.createInstrumentedTransaction(context)) {
                BucketMetadata metadata = BucketMetadataUtil.open(context, tr, session, message.getBucket());
                checkBucketOwnership(metadata);

                VectorIndex vectorIndex = metadata.vectorIndexes().getIndexBySelector(
                        message.getSelector(), IndexSelectionPolicy.ALL
                );
                if (vectorIndex == null) {
                    throw new KronotopException("No vector index found for selector: '" + message.getSelector() + "'");
                }

                float[] vector = parseVector(message.getVector(), vectorIndex.definition().dimensions());
                validateVector(vectorIndex, vector);

                String namespace = session.attr(SessionAttributes.CURRENT_NAMESPACE).get();
                VectorGraphIndexGroup group = service.getVectorGraphRegistry().get(
                        namespace,
                        message.getBucket(),
                        vectorIndex.definition().id()
                );

                if (group == null) {
                    group = service.getVectorGraphRegistry().computeIfAbsent(
                            metadata,
                            vectorIndex,
                            () -> service.bootstrapVectorGroup(metadata, vectorIndex)
                    );
                }

                if (!group.isReady()) {
                    throw new VectorIndexNotReadyException(namespace, message.getBucket(), vectorIndex.definition().id());
                }

                if (group.isEmpty()) {
                    return List.<VectorSearchResult>of();
                }

                int topK = message.getTopK() == 0 ? DEFAULT_TOPK : message.getTopK();
                float threshold = message.getThreshold();
                float overquery = message.getOverquery() > 0
                        ? message.getOverquery()
                        : service.getDefaultOverquery();

                List<VectorSearchResult> results;
                int maxScanCandidates = message.getMaxScanCandidates() > 0
                        ? message.getMaxScanCandidates()
                        : service.getMaxScanCandidates();

                if (message.getFilter() == null) {
                    results = fetchResults(group, vector, topK, threshold, overquery);
                } else {
                    results = fetchFilteredResults(group, vector, topK, threshold, overquery, metadata, session, message.getFilter(), maxScanCandidates);
                }

                byte[] projectionBytes = message.getProjection();
                if (projectionBytes != null) {
                    BsonDocument projectionSpec = BSONUtil.fromJson(projectionBytes);
                    BqlExpr parsedQuery = message.getFilter() != null ? BqlParser.parse(message.getFilter()) : null;
                    results = applyVectorProjection(results, projectionSpec, parsedQuery);
                }

                return results;
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }, (results) -> {
            RESPVersion protoVer = request.getSession().protocolVersion();
            if (protoVer.equals(RESPVersion.RESP3)) {
                resp3VectorResponse(request, response, results);
            } else if (protoVer.equals(RESPVersion.RESP2)) {
                resp2VectorResponse(request, response, results);
            } else {
                throw new KronotopException("Unknown protocol version " + protoVer.getValue());
            }
        });
    }

    private List<VectorSearchResult> fetchResults(VectorGraphIndexGroup group, float[] vector, int topK, float threshold, float overquery) throws IOException {
        List<MergedNodeScore> mergedResults = group.searchAll(vector, topK, threshold, overquery);
        List<VectorSearchResult> results = new ArrayList<>();
        for (MergedNodeScore merged : mergedResults) {
            BucketShard targetShard = service.getShard(merged.location().shardId());
            if (targetShard == null) {
                throw new KronotopException("Shard not found: " + merged.location().shardId());
            }
            ByteBuffer document = targetShard.volume().getByEntryMetadata(merged.location().entryMetadata());
            results.add(new VectorSearchResult(merged.score(), document));
        }
        return results;
    }

    private List<VectorSearchResult> fetchFilteredResults(
            VectorGraphIndexGroup group, float[] vector, int topK, float threshold, float overquery,
            BucketMetadata metadata, Session session, byte[] filterBytes, int maxScanCandidates) throws IOException {

        int initialK = (int) Math.ceil(topK * 1.5);
        Map<ObjectId, Float> scoreMap = new HashMap<>();

        VectorSearchSession searchSession = group.createSearchSession(vector);
        try (VectorCandidateSupplier supplier = new VectorCandidateSupplier(searchSession, initialK, threshold, scoreMap, maxScanCandidates, overquery)) {
            MaterializedPlan materializedPlan = MaterializedPlanBuilder.build(
                    context, metadata, service.getPlanner(), filterBytes, supplier, planCacheEnabled, planCacheMaxTtl
            );
            if (materializedPlan == null) {
                return List.of();
            }

            QueryOptions options = QueryOptions.builder().limit(topK).build();
            QueryContext ctx = new QueryContext(session, metadata, options, materializedPlan.plan(), materializedPlan.parameters());

            List<PersistedEntry> entries = service.getQueryExecutor().materializedRead(ctx);

            List<VectorSearchResult> results = new ArrayList<>(entries.size());
            for (PersistedEntry entry : entries) {
                Float score = scoreMap.get(entry.objectId());
                results.add(new VectorSearchResult(score != null ? score : 0f, entry.document()));
            }

            // Sort by score descending
            results.sort((a, b) -> Float.compare(b.score(), a.score()));
            return results;
        }
    }
}
