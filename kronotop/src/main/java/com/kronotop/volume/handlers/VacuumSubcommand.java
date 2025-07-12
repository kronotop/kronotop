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

import com.apple.foundationdb.Transaction;
import com.kronotop.KronotopException;
import com.kronotop.cluster.handlers.InvalidNumberOfParametersException;
import com.kronotop.internal.ProtocolMessageUtil;
import com.kronotop.redis.server.SubcommandHandler;
import com.kronotop.server.Request;
import com.kronotop.server.Response;
import com.kronotop.task.TaskService;
import com.kronotop.volume.*;
import io.netty.buffer.ByteBuf;

import java.util.ArrayList;

class VacuumSubcommand extends BaseSubcommandHandler implements SubcommandHandler {
    VacuumSubcommand(VolumeService service) {
        super(service);
    }

    private VacuumMetadata newVacuumMetadata(Volume volume, VacuumParameters parameters) {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VacuumMetadata vacuumMetadata = VacuumMetadata.load(tr, volume.getConfig().subspace());
            if (vacuumMetadata != null) {
                throw new KronotopException("Vacuum task on volume " + volume.getConfig().name() + " already exists");
            }
            vacuumMetadata = new VacuumMetadata(volume.getConfig().name(), parameters.allowedGarbageRatio);
            vacuumMetadata.save(tr, volume.getConfig().subspace());
            tr.commit().join();
            return vacuumMetadata;
        }
    }

    @Override
    public void execute(Request request, Response response) {
        VacuumParameters parameters = new VacuumParameters(request.getParams());
        try {
            Volume volume = service.findVolume(parameters.volumeName);
            if (!service.hasVolumeOwnership(volume)) {
                throw new KronotopException("Volume " + volume.getConfig().name() + " is not owned by this member");
            }
            VacuumMetadata vacuumMetadata = newVacuumMetadata(volume, parameters);
            TaskService taskService = context.getService(TaskService.NAME);
            VacuumTask task = new VacuumTask(service.getContext(), volume, vacuumMetadata);
            taskService.execute(task);
        } catch (ClosedVolumeException | VolumeNotOpenException e) {
            throw new KronotopException(e);
        }
        response.writeOK();
    }

    private static class VacuumParameters {
        private final String volumeName;
        private final double allowedGarbageRatio;

        private VacuumParameters(ArrayList<ByteBuf> params) {
            if (params.size() != 3) {
                throw new InvalidNumberOfParametersException();
            }

            volumeName = ProtocolMessageUtil.readAsString(params.get(1));
            allowedGarbageRatio = ProtocolMessageUtil.readAsDouble(params.get(2));
        }
    }
}
