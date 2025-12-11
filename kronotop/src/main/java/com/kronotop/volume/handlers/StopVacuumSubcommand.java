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
import com.kronotop.task.Task;
import com.kronotop.task.TaskService;
import com.kronotop.volume.VacuumMetadata;
import com.kronotop.volume.Volume;
import com.kronotop.volume.VolumeService;
import io.netty.buffer.ByteBuf;

import java.util.ArrayList;

import static com.kronotop.AsyncCommandExecutor.runAsync;

public class StopVacuumSubcommand extends BaseSubcommandHandler implements SubcommandHandler {

    public StopVacuumSubcommand(VolumeService service) {
        super(service);
    }

    @Override
    public void execute(Request request, Response response) {
        StopVacuumParameters parameters = new StopVacuumParameters(request.getParams());
        runAsync(context, response, () -> {
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                Volume volume = service.findVolume(parameters.volumeName);
                VacuumMetadata vacuumMetadata = VacuumMetadata.load(tr, volume.getConfig().subspace());
                if (vacuumMetadata == null) {
                    throw new KronotopException("Vacuum task not found on " + volume.getConfig().name());
                }
                TaskService taskService = context.getService(TaskService.NAME);
                Task task = taskService.getTask(vacuumMetadata.getTaskName());
                task.shutdown(); // stop it gracefully
                task.complete(); // delete VacuumMetadata from FDB, destroy the task.
            }
        }, response::writeOK);
    }

    private static class StopVacuumParameters {
        private final String volumeName;

        private StopVacuumParameters(ArrayList<ByteBuf> params) {
            if (params.size() != 2) {
                throw new InvalidNumberOfParametersException();
            }

            volumeName = ProtocolMessageUtil.readAsString(params.get(1));
        }
    }
}
