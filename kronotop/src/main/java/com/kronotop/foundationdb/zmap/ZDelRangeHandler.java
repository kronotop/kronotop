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

import com.apple.foundationdb.Range;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.subspace.Subspace;
import com.kronotop.foundationdb.zmap.protocol.ZDelRangeMessage;
import com.kronotop.server.Handler;
import com.kronotop.server.MessageTypes;
import com.kronotop.server.Request;
import com.kronotop.server.Response;
import com.kronotop.server.annotation.Command;
import com.kronotop.server.annotation.MaximumParameterCount;
import com.kronotop.server.annotation.MinimumParameterCount;

@Command(ZDelRangeMessage.COMMAND)
@MinimumParameterCount(ZDelRangeMessage.MINIMUM_PARAMETER_COUNT)
@MaximumParameterCount(ZDelRangeMessage.MAXIMUM_PARAMETER_COUNT)
class ZDelRangeHandler extends BaseZMapHandler implements Handler {
    public ZDelRangeHandler(ZMapService service) {
        super(service);
    }

    @Override
    public boolean isWatchable() {
        return true;
    }

    @Override
    public void beforeExecute(Request request) {
        request.attr(MessageTypes.ZDELRANGE).set(new ZDelRangeMessage(request));
    }

    @Override
    public void execute(Request request, Response response) {
        // Validates the request
        ZDelRangeMessage zDelRangeMessage = request.attr(MessageTypes.ZDELRANGE).get();

        Subspace subspace = getSubspace(response, zDelRangeMessage.getNamespace());
        Transaction transaction = getOrCreateTransaction(response);
        byte[] begin = subspace.pack(zDelRangeMessage.getBegin());
        byte[] end = subspace.pack(zDelRangeMessage.getEnd());
        Range range = new Range(begin, end);
        transaction.clear(range);
        if (isOneOff(response)) {
            transaction.commit().join();
        }
        response.writeOK();
    }
}