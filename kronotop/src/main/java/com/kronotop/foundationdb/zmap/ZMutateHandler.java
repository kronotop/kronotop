/*
 * Copyright (c) 2023 Kronotop
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

package com.kronotop.foundationdb.zmap;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.subspace.Subspace;
import com.kronotop.foundationdb.FoundationDBService;
import com.kronotop.foundationdb.zmap.protocol.ZMutateMessage;
import com.kronotop.server.Handler;
import com.kronotop.server.MessageTypes;
import com.kronotop.server.Request;
import com.kronotop.server.Response;
import com.kronotop.server.annotation.Command;
import com.kronotop.server.annotation.MaximumParameterCount;
import com.kronotop.server.annotation.MinimumParameterCount;

import java.util.Collections;
import java.util.List;

@Command(ZMutateMessage.COMMAND)
@MinimumParameterCount(ZMutateMessage.MINIMUM_PARAMETER_COUNT)
@MaximumParameterCount(ZMutateMessage.MAXIMUM_PARAMETER_COUNT)
public class ZMutateHandler extends BaseZMapHandler implements Handler {
    public ZMutateHandler(FoundationDBService service) {
        super(service);
    }

    @Override
    public boolean isWatchable() {
        return true;
    }

    @Override
    public List<String> getKeys(Request request) {
        return Collections.singletonList(new String(request.attr(MessageTypes.ZMUTATE).get().getKey()));
    }

    @Override
    public void beforeExecute(Request request) {
        request.attr(MessageTypes.ZMUTATE).set(new ZMutateMessage(request));
    }

    @Override
    public void execute(Request request, Response response) {
        // Validates the request
        ZMutateMessage zMutateMessage = request.attr(MessageTypes.ZMUTATE).get();
        Subspace subspace = getSubspace(response, zMutateMessage.getNamespace());

        Transaction tr = getOrCreateTransaction(response);
        tr.mutate(zMutateMessage.getMutationType(), subspace.pack(zMutateMessage.getKey()), zMutateMessage.getParam());
        if (isOneOff(response)) {
            tr.commit().join();
        }
        response.writeOK();
    }
}
