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

import com.kronotop.server.Handler;
import com.kronotop.server.MessageTypes;
import com.kronotop.server.Request;
import com.kronotop.server.Response;
import com.kronotop.server.annotation.Command;
import com.kronotop.server.annotation.MinimumParameterCount;
import com.kronotop.volume.VolumeService;
import com.kronotop.volume.handlers.protocol.VolumeWatchMessage;

import static com.kronotop.AsyncCommandExecutor.supplyAsync;

@Command(VolumeWatchMessage.COMMAND)
@MinimumParameterCount(VolumeWatchMessage.MINIMUM_PARAMETER_COUNT)
public class VolumeWatchHandler extends BaseVolumeHandler implements Handler {
    public VolumeWatchHandler(VolumeService service) {
        super(service);
    }

    @Override
    public void beforeExecute(Request request) {
        request.attr(MessageTypes.VOLUMEWATCH).set(new VolumeWatchMessage(request));
    }

    @Override
    public void execute(Request request, Response response) throws Exception {
        // Volume'un change log'unu okumasi lazim volume'u acmadan. pratikte her node ustunde calismali bu.
        // Hemen change log'a bakip en buyuk LSN'i bulmali.
        // Eger kullanicinin LSN'inden buyukse bulunan LSN aninda geri donmesi lazim bu guncel LSN'i.
        // Eger kullanicinin LSN'i ile bulunan en buyuk LSN esitse beklemesi lazim kendini merkezi bir watcher'a register ederek.
        // Merkezi watch'er tarafindan uyandirilinca bir TX acip en guncel LSN'i bulup client'ina donmeli.
        // Tek bir FDB watch'in ayni anda birden fazla thread'i uyandiracagi ucuz ve etkili bir mekanizma bulmamiz lazim. FDB watcher'i hemen kendini tekrar kuracak.
        // FDB watch'ler geri dondugunde hemen subscriber'larini sinyallemesi lazim.
        supplyAsync(context, response, () -> {
            VolumeWatchMessage message = request.attr(MessageTypes.VOLUMEWATCH).get();
            System.out.println(message.getVolume());
            System.out.println(message.getLogSequenceNumber());
            return 1L;
        }, response::writeInteger);
    }
}
