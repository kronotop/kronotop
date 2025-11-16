/*
 * Copyright (c) 2023-2025 Burak Sezer
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

import com.kronotop.cluster.handlers.InvalidNumberOfParametersException;
import com.kronotop.internal.ProtocolMessageUtil;
import com.kronotop.redis.server.SubcommandHandler;
import com.kronotop.server.Request;
import com.kronotop.server.Response;
import com.kronotop.volume.SegmentUtil;
import com.kronotop.volume.Volume;
import com.kronotop.volume.VolumeConfig;
import com.kronotop.volume.VolumeService;
import io.netty.buffer.ByteBuf;

import java.util.ArrayList;

class FindPosition extends BaseSubcommandHandler implements SubcommandHandler {

    public FindPosition(VolumeService service) {
        super(service);
    }

    @Override
    public void execute(Request request, Response response) {
        FindPositionParameters parameters = new FindPositionParameters(request.getParams());
        Volume volume = service.findVolume(parameters.name);
        VolumeConfig config = volume.getConfig();

        long position = SegmentUtil.findPosition(context, config.subspace(), parameters.segmentId);
        response.writeInteger(position);
    }

    private static class FindPositionParameters {
        private final String name;
        private final long segmentId;

        private FindPositionParameters(ArrayList<ByteBuf> params) {
            if (params.size() != 3) {
                throw new InvalidNumberOfParametersException();
            }

            name = ProtocolMessageUtil.readAsString(params.get(1));
            segmentId = ProtocolMessageUtil.readAsLong(params.get(2));
        }
    }
}
