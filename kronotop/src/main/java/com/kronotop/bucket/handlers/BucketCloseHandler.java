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

package com.kronotop.bucket.handlers;

import com.kronotop.bucket.BucketService;
import com.kronotop.bucket.handlers.protocol.BucketCloseMessage;
import com.kronotop.bucket.pipeline.QueryContext;
import com.kronotop.server.MessageTypes;
import com.kronotop.server.Request;
import com.kronotop.server.Response;
import com.kronotop.server.annotation.Command;
import com.kronotop.server.annotation.MaximumParameterCount;
import com.kronotop.server.annotation.MinimumParameterCount;

import java.util.Map;

@Command(BucketCloseMessage.COMMAND)
@MaximumParameterCount(BucketCloseMessage.MAXIMUM_PARAMETER_COUNT)
@MinimumParameterCount(BucketCloseMessage.MAXIMUM_PARAMETER_COUNT)
public class BucketCloseHandler extends AbstractBucketHandler {

    public BucketCloseHandler(BucketService service) {
        super(service);
    }

    @Override
    public void beforeExecute(Request request) {
        request.attr(MessageTypes.BUCKETCLOSE).set(new BucketCloseMessage(request));
    }

    @Override
    public void execute(Request request, Response response) throws Exception {
        BucketCloseMessage message = request.attr(MessageTypes.BUCKETCLOSE).get();
        Map<Integer, QueryContext> contexts = findQueryContext(request.getSession(), message.getOperation());
        QueryContext ctx = contexts.remove(message.getCursorId());
        if (ctx == null) {
            response.writeError("no cursor found");
            return;
        }
        // With the current implementation, there is no need to release context manually
        // GC will throw it away.
        response.writeOK();
    }
}
