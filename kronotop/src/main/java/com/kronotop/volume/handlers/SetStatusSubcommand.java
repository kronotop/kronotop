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
import com.kronotop.cluster.handlers.InvalidNumberOfParametersException;
import com.kronotop.internal.ByteBufUtils;
import com.kronotop.redis.server.SubcommandHandler;
import com.kronotop.server.Request;
import com.kronotop.server.Response;
import com.kronotop.volume.*;
import io.netty.buffer.ByteBuf;

import java.util.ArrayList;

public class SetStatusSubcommand extends BaseSubcommandHandler implements SubcommandHandler {

    public SetStatusSubcommand(VolumeService service) {
        super(service);
    }

    @Override
    public void execute(Request request, Response response) {
        SetStatusParameters parameters = new SetStatusParameters(request.getParams());
        try {
            Volume volume = service.findVolume(parameters.name);
            volume.setStatus(parameters.status);
            response.writeOK();
        } catch (VolumeNotOpenException | ClosedVolumeException e) {
            throw new KronotopException(e);
        }
    }

    private class SetStatusParameters {
        private final String name;
        private final VolumeStatus status;

        private SetStatusParameters(ArrayList<ByteBuf> params) {
            if (params.size() != 3) {
                throw new InvalidNumberOfParametersException();
            }

            name = ByteBufUtils.readAsString(params.get(1));
            String rawStatus = ByteBufUtils.readAsString(params.get(2));
            try {
                status = VolumeStatus.valueOf(rawStatus.toUpperCase());
            } catch (IllegalArgumentException e) {
                throw new KronotopException("Invalid volume status: " + rawStatus);
            }
        }
    }
}
