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
import com.kronotop.volume.Volume;
import com.kronotop.volume.VolumeService;
import com.kronotop.volume.handlers.protocol.SegmentInsertMessage;

import java.io.IOException;

import static com.kronotop.AsyncCommandExecutor.runAsync;

@Command(SegmentInsertMessage.COMMAND)
@MinimumParameterCount(SegmentInsertMessage.MINIMUM_PARAMETER_COUNT)
public class SegmentInsertHandler extends BaseVolumeHandler implements Handler {

    public SegmentInsertHandler(VolumeService service) {
        super(service);
    }

    @Override
    public void beforeExecute(Request request) {
        request.attr(MessageTypes.SEGMENTINSERT).set(new SegmentInsertMessage(request));
    }

    @Override
    public void execute(Request request, Response response) {
        runAsync(context, response, () -> {
            SegmentInsertMessage message = request.attr(MessageTypes.SEGMENTINSERT).get();
            try {
                Volume volume = service.findVolume(message.getVolume());
                volume.insert(message.getSegmentId(), message.getPackedEntries());
                volume.flush();
            } catch (IOException e) {
                throw new KronotopException(e.getMessage(), e);
            }
        }, response::writeOK);
    }
}
