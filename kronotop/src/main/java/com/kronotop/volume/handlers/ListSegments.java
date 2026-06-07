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
import com.kronotop.cluster.handlers.InvalidNumberOfParametersException;
import com.kronotop.internal.ProtocolMessageUtil;
import com.kronotop.server.Request;
import com.kronotop.server.Response;
import com.kronotop.server.SubcommandHandler;
import com.kronotop.server.resp3.IntegerRedisMessage;
import com.kronotop.server.resp3.RedisMessage;
import com.kronotop.transaction.TransactionUtil;
import com.kronotop.volume.*;
import io.netty.buffer.ByteBuf;

import java.util.ArrayList;
import java.util.List;

import static com.kronotop.AsyncCommandExecutor.supplyAsync;

public class ListSegments extends BaseSubcommandHandler implements SubcommandHandler {

    public ListSegments(VolumeService service) {
        super(service);
    }

    @Override
    public void execute(Request request, Response response) {
        ListSegmentsParameters parameters = new ListSegmentsParameters(request.getParams());
        supplyAsync(context, response, () -> {
            Volume volume = service.findVolume(parameters.name);
            VolumeConfig config = volume.getConfig();

            List<RedisMessage> children = new ArrayList<>();
            try (Transaction tr = TransactionUtil.createInstrumentedTransaction(context)) {
                List<Long> segmentIds = VolumeMetadataUtil.loadSegmentIds(tr, new VolumeSubspace(config.subspace()));
                for (long segmentId : segmentIds) {
                    children.add(new IntegerRedisMessage(segmentId));
                }
            }
            return children;
        }, response::writeArray);
    }

    private static class ListSegmentsParameters {
        private final String name;

        private ListSegmentsParameters(ArrayList<ByteBuf> params) {
            if (params.size() != 2) {
                throw new InvalidNumberOfParametersException();
            }

            name = ProtocolMessageUtil.readAsString(params.get(1));
        }
    }
}
