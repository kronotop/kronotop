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

package com.kronotop.redis.connection;

import com.kronotop.common.resp.RESPError;
import com.kronotop.redis.RedisService;
import com.kronotop.redis.connection.protocol.AuthMessage;
import com.kronotop.server.*;
import com.kronotop.server.annotation.Command;
import com.kronotop.server.annotation.MaximumParameterCount;
import com.kronotop.server.annotation.MinimumParameterCount;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;
import io.netty.util.Attribute;

import java.util.Objects;

@Command(AuthMessage.COMMAND)
@MinimumParameterCount(AuthMessage.MINIMUM_PARAMETER_COUNT)
@MaximumParameterCount(AuthMessage.MAXIMUM_PARAMETER_COUNT)
public class AuthHandler implements Handler {
    private final RedisService service;

    public AuthHandler(RedisService service) {
        this.service = service;
    }

    private void writeWrongPassErr(Response response) {
        response.writeError(
                RESPError.WRONGPASS,
                "invalid username-password pair or user is disabled.");
    }

    private void authAttrSet(Response response) {
        Attribute<Boolean> authAttr = response.
                getChannelContext().
                channel().
                attr(ChannelAttributes.AUTH);
        authAttr.set(true);
    }

    @Override
    public void beforeExecute(Request request) {
        request.attr(MessageTypes.AUTH).set(new AuthMessage(request));
    }

    @Override
    public void execute(Request request, Response response) {
        AuthMessage msg = request.attr(MessageTypes.AUTH).get();

        Config config = service.getContext().getConfig();
        if (config.hasPath("auth.requirepass")) {
            String password = config.getString("auth.requirepass");
            if (!Objects.equals(password, msg.getPassword())) {
                writeWrongPassErr(response);
                return;
            }
            // Authenticated
            authAttrSet(response);
            response.writeOK();
            return;
        }

        if (request.getParams().size() == 1) {
            response.writeError("AUTH <password> called without any password configured for the default user. Are you sure your configuration is correct?");
            return;
        }

        Config authConfig;
        try {
            authConfig = config.getConfig("auth.users");
        } catch (ConfigException.Missing e) {
            writeWrongPassErr(response);
            return;
        } catch (Exception e) {
            response.writeError(e.getMessage());
            return;
        }

        try {
            String password = authConfig.getString(msg.getUsername());
            if (!Objects.equals(password, msg.getPassword())) {
                writeWrongPassErr(response);
                return;
            }
        } catch (ConfigException.Missing e) {
            writeWrongPassErr(response);
            return;
        }

        // Authenticated
        authAttrSet(response);
        response.writeOK();
    }
}
