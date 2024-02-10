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

package com.kronotop.foundationdb;

import com.kronotop.foundationdb.protocol.SnapshotReadMessage;
import com.kronotop.server.*;
import com.kronotop.server.annotation.Command;
import com.kronotop.server.annotation.MaximumParameterCount;
import com.kronotop.server.annotation.MinimumParameterCount;
import io.netty.channel.Channel;
import io.netty.util.Attribute;

@Command(SnapshotReadMessage.COMMAND)
@MaximumParameterCount(SnapshotReadMessage.MAXIMUM_PARAMETER_COUNT)
@MinimumParameterCount(SnapshotReadMessage.MINIMUM_PARAMETER_COUNT)
class SnapshotReadHandler implements Handler {
    private final FoundationDBService service;

    SnapshotReadHandler(FoundationDBService service) {
        this.service = service;
    }

    @Override
    public void beforeExecute(Request request) {
        request.attr(MessageTypes.SNAPSHOT_READ).set(new SnapshotReadMessage(request));
    }

    @Override
    public void execute(Request request, Response response) {
        SnapshotReadMessage snapshotReadMessage = request.attr(MessageTypes.SNAPSHOT_READ).get();
        Channel channel = response.getChannelContext().channel();
        Attribute<Boolean> snapshotReadAttr = channel.attr(ChannelAttributes.SNAPSHOT_READ);
        if (snapshotReadMessage.getOption().equals(SnapshotReadMessage.ON_KEYWORD)) {
            snapshotReadAttr.set(true);
        } else {
            snapshotReadAttr.set(null);
        }

        response.writeOK();
    }
}
