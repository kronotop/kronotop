/*
 * Copyright (c) 2023-2024 Kronotop
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

import com.kronotop.cluster.handlers.InvalidNumberOfParametersException;
import com.kronotop.common.KronotopException;
import com.kronotop.redis.server.SubcommandHandler;
import com.kronotop.server.Request;
import com.kronotop.server.Response;
import com.kronotop.volume.ClosedVolumeException;
import com.kronotop.volume.Volume;
import com.kronotop.volume.VolumeNotOpenException;
import com.kronotop.volume.VolumeService;
import io.netty.buffer.ByteBuf;

import java.util.ArrayList;

class VacuumSubcommand extends BaseHandler implements SubcommandHandler {

    VacuumSubcommand(VolumeService service) {
        super(service);
    }

    @Override
    public void execute(Request request, Response response) {
        VacuumParameters parameters = new VacuumParameters(request.getParams());
        try {
            Volume volume = service.findVolume(parameters.volumeName);
        } catch (ClosedVolumeException | VolumeNotOpenException e) {
            throw new KronotopException(e);
        }
    }

    private class VacuumParameters {
        private final String volumeName;
        private final double maximumGarbageRatio;

        private VacuumParameters(ArrayList<ByteBuf> params) {
            if (params.size() != 3) {
                throw new InvalidNumberOfParametersException();
            }

            volumeName = readAsString(params.get(1));
            String rawMaximumGarbageRatio = readAsString(params.get(2));
            maximumGarbageRatio = Double.parseDouble(rawMaximumGarbageRatio);
        }
    }
}