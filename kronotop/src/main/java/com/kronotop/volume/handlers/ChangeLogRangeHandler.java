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

package com.kronotop.volume.handlers;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.kronotop.internal.VersionstampUtil;
import com.kronotop.server.Handler;
import com.kronotop.server.MessageTypes;
import com.kronotop.server.Request;
import com.kronotop.server.Response;
import com.kronotop.server.annotation.Command;
import com.kronotop.server.annotation.MaximumParameterCount;
import com.kronotop.server.annotation.MinimumParameterCount;
import com.kronotop.server.resp3.IntegerRedisMessage;
import com.kronotop.server.resp3.MapRedisMessage;
import com.kronotop.server.resp3.RedisMessage;
import com.kronotop.transaction.TransactionUtil;
import com.kronotop.volume.VolumeService;
import com.kronotop.volume.changelog.*;
import com.kronotop.volume.handlers.protocol.ChangeLogRangeMessage;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static com.kronotop.AsyncCommandExecutor.supplyAsync;
import static com.kronotop.server.RESPUtil.bulkString;
import static com.kronotop.server.RESPUtil.wrapBytes;

@Command(ChangeLogRangeMessage.COMMAND)
@MaximumParameterCount(ChangeLogRangeMessage.MAXIMUM_PARAMETER_COUNT)
@MinimumParameterCount(ChangeLogRangeMessage.MINIMUM_PARAMETER_COUNT)
public class ChangeLogRangeHandler extends BaseVolumeHandler implements Handler {
    private static final int DEFAULT_LIMIT = 1000;

    // Pre-computed key bytes to avoid per-entry allocations
    private static final byte[] SEQUENCE_NUMBER_BYTES = "sequence_number".getBytes(StandardCharsets.UTF_8);
    private static final byte[] SEGMENT_ID_BYTES = "segment_id".getBytes(StandardCharsets.UTF_8);
    private static final byte[] POSITION_BYTES = "position".getBytes(StandardCharsets.UTF_8);
    private static final byte[] LENGTH_BYTES = "length".getBytes(StandardCharsets.UTF_8);
    private static final byte[] VERSIONSTAMP_BYTES = "versionstamp".getBytes(StandardCharsets.UTF_8);
    private static final byte[] KIND_BYTES = "kind".getBytes(StandardCharsets.UTF_8);
    private static final byte[] PREFIX_BYTES = "prefix".getBytes(StandardCharsets.UTF_8);
    private static final byte[] BEFORE_BYTES = "before".getBytes(StandardCharsets.UTF_8);
    private static final byte[] AFTER_BYTES = "after".getBytes(StandardCharsets.UTF_8);

    public ChangeLogRangeHandler(VolumeService service) {
        super(service);
    }

    @Override
    public void beforeExecute(Request request) {
        request.attr(MessageTypes.CHANGELOGRANGE).set(new ChangeLogRangeMessage(request));
    }

    private RedisMessage mapCoordinate(ChangeLogCoordinate coordinate) {
        Map<RedisMessage, RedisMessage> map = new LinkedHashMap<>();
        map.put(wrapBytes(SEQUENCE_NUMBER_BYTES), new IntegerRedisMessage(coordinate.sequenceNumber()));
        map.put(wrapBytes(SEGMENT_ID_BYTES), new IntegerRedisMessage(coordinate.segmentId()));
        map.put(wrapBytes(POSITION_BYTES), new IntegerRedisMessage(coordinate.position()));
        map.put(wrapBytes(LENGTH_BYTES), new IntegerRedisMessage(coordinate.length()));
        return new MapRedisMessage(map);
    }

    private RedisMessage mapEntry(ChangeLogEntry entry) {
        Map<RedisMessage, RedisMessage> map = new LinkedHashMap<>();

        map.put(
                wrapBytes(VERSIONSTAMP_BYTES),
                bulkString(VersionstampUtil.base32HexEncode(entry.getVersionstamp()))
        );
        map.put(wrapBytes(KIND_BYTES), bulkString(entry.getKind().toString()));
        map.put(wrapBytes(PREFIX_BYTES), new IntegerRedisMessage(entry.getPrefix()));

        if (entry.hasBefore()) {
            map.put(wrapBytes(BEFORE_BYTES), mapCoordinate(entry.getBefore().orElseThrow()));
        }

        if (entry.hasAfter()) {
            map.put(wrapBytes(AFTER_BYTES), mapCoordinate(entry.getAfter().orElseThrow()));
        }

        return new MapRedisMessage(map);
    }

    @Override
    public void execute(Request request, Response response) throws Exception {
        supplyAsync(context, response, () -> {
            ChangeLogRangeMessage message = request.attr(MessageTypes.CHANGELOGRANGE).get();
            if (message.getStart() >= message.getEnd()) {
                throw new IllegalArgumentException("start cannot be greater or equal to end");
            }

            SequenceNumberSelector start = message.isStartInclusive() ?
                    SequenceNumberSelector.firstGreaterOrEqual(message.getStart()) :
                    SequenceNumberSelector.firstGreaterThan(message.getStart());

            SequenceNumberSelector end = message.isEndInclusive() ?
                    SequenceNumberSelector.firstGreaterThan(message.getEnd()) :
                    SequenceNumberSelector.firstGreaterOrEqual(message.getEnd());

            int limit = message.getLimit() > 0 ? message.getLimit() : DEFAULT_LIMIT;
            ChangeLogIterableOptions options = new ChangeLogIterableOptions.Builder().
                    begin(start).
                    end(end).
                    limit(limit).
                    parentOperationKind(message.getParentOpKind()).
                    reverse(message.isReverse()).
                    build();

            DirectorySubspace subspace = service.openSubspace(message.getVolume());

            List<RedisMessage> entries = new ArrayList<>();
            try (Transaction tr = TransactionUtil.createInstrumentedTransaction(context)) {
                ChangeLogIterable iterable = new ChangeLogIterable(tr, subspace, options);
                for (ChangeLogEntry entry : iterable) {
                    entries.add(mapEntry(entry));
                }
            }
            return entries;
        }, response::writeArray);
    }
}
