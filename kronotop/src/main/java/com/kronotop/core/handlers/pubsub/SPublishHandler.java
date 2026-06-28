/*
 * Copyright (c) 2023-2026 Burak Sezer
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

package com.kronotop.core.handlers.pubsub;

import com.kronotop.core.handlers.pubsub.protocol.SPublishMessage;
import com.kronotop.core.pubsub.SubscriptionRegistry;
import com.kronotop.server.*;
import com.kronotop.server.annotation.Command;
import com.kronotop.server.annotation.MaximumParameterCount;
import com.kronotop.server.annotation.MinimumParameterCount;

@Command(SPublishMessage.COMMAND)
@MinimumParameterCount(SPublishMessage.MINIMUM_PARAMETER_COUNT)
@MaximumParameterCount(SPublishMessage.MAXIMUM_PARAMETER_COUNT)
public class SPublishHandler implements Handler {
    private final SubscriptionRegistry registry;

    public SPublishHandler(SubscriptionRegistry registry) {
        this.registry = registry;
    }

    @Override
    public boolean isRedisCompatible() {
        return false;
    }

    @Override
    public void beforeExecute(Request request) {
        request.attr(MessageTypes.SPUBLISH).set(new SPublishMessage(request));
    }

    @Override
    public void execute(Request request, Response response) {
        SPublishMessage message = request.attr(MessageTypes.SPUBLISH).get();
        String channel = message.getKey();

        registry.ensurePrimary(channel);
        String namespace = request.getSession().attr(SessionAttributes.CURRENT_NAMESPACE).get();
        int delivered = registry.publish(namespace, channel, message.getMessage());
        response.writeInteger(delivered);
    }
}
