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

package com.kronotop.volume.handlers;

import com.kronotop.KronotopException;
import com.kronotop.server.Handler;
import com.kronotop.server.MessageTypes;
import com.kronotop.server.Request;
import com.kronotop.server.Response;
import com.kronotop.server.annotation.Command;
import com.kronotop.server.annotation.MinimumParameterCount;
import com.kronotop.server.resp3.FullBulkStringRedisMessage;
import com.kronotop.server.resp3.RedisMessage;
import com.kronotop.volume.ClosedVolumeException;
import com.kronotop.volume.Volume;
import com.kronotop.volume.VolumeNotOpenException;
import com.kronotop.volume.VolumeService;
import com.kronotop.volume.handlers.protocol.SegmentRangeMessage;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static com.kronotop.AsyncCommandExecutor.supplyAsync;

@Command(SegmentRangeMessage.COMMAND)
@MinimumParameterCount(SegmentRangeMessage.MINIMUM_PARAMETER_COUNT)
public class SegmentRangeHandler extends BaseVolumeHandler implements Handler {
    public SegmentRangeHandler(VolumeService service) {
        super(service);
    }

    @Override
    public void beforeExecute(Request request) {
        request.attr(MessageTypes.SEGMENTRANGE).set(new SegmentRangeMessage(request));
    }

    @Override
    public void execute(Request request, Response response) throws Exception {
        supplyAsync(context, response, () -> {
            SegmentRangeMessage message = request.attr(MessageTypes.SEGMENTRANGE).get();
            try {
                Volume volume = service.findVolume(message.getVolume());
                List<RedisMessage> children = new ArrayList<>();
                ByteBuffer[] entries = volume.getSegmentRange(message.getSegmentId(), message.getSegmentRanges());
                for (ByteBuffer entry : entries) {
                    ByteBuf buffer = Unpooled.wrappedBuffer(entry);
                    children.add(new FullBulkStringRedisMessage(buffer));
                }
                return children;
            } catch (VolumeNotOpenException | ClosedVolumeException | IOException e) {
                throw new KronotopException(e.getMessage(), e);
            }
        }, response::writeArray);
    }
}
