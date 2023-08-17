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

package com.kronotop.redis.connection;

import com.kronotop.common.resp.RESPError;
import com.kronotop.core.network.clients.Client;
import com.kronotop.core.network.clients.Clients;
import com.kronotop.redis.BaseHandler;
import com.kronotop.redis.RedisService;
import com.kronotop.redis.connection.protocol.HelloMessage;
import com.kronotop.server.resp.*;
import com.kronotop.server.resp.annotation.Command;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;
import io.netty.handler.codec.redis.ArrayRedisMessage;
import io.netty.handler.codec.redis.IntegerRedisMessage;
import io.netty.handler.codec.redis.RedisMessage;
import io.netty.handler.codec.redis.SimpleStringRedisMessage;
import io.netty.util.Attribute;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;


@Command(HelloMessage.COMMAND)
public class HelloHandler extends BaseHandler implements Handler {
    public HelloHandler(RedisService service) {
        super(service);
    }

    @Override
    public void beforeExecute(Request request) {
        request.attr(MessageTypes.HELLO).set(new HelloMessage(request));
    }

    private void writeWrongPassErr(Response response) {
        response.writeError(
                RESPError.WRONGPASS,
                "invalid username-password pair or user is disabled.");
    }

    private void authAttrSet(Response response) {
        Attribute<Boolean> authAttr = response.
                getContext().
                channel().
                attr(ChannelAttributes.AUTH);
        authAttr.set(true);
    }

    @Override
    public void execute(Request request, Response response) throws Exception {
        HelloMessage msg = request.attr(MessageTypes.HELLO).get();

        Config config = service.getContext().getConfig();
        if (msg.hasAuth()) {
            if (config.hasPath("auth.requirepass")) {
                String password = config.getString("auth.requirepass");
                if (msg.getUsername().equals("default") && Objects.equals(password, msg.getPassword())) {
                    // Authenticated
                    authAttrSet(response);
                } else {
                    writeWrongPassErr(response);
                    return;
                }
            } else {
                try {
                    Config authConfig = config.getConfig("auth.users");
                    String password = authConfig.getString(msg.getUsername());
                    if (!Objects.equals(password, msg.getPassword())) {
                        writeWrongPassErr(response);
                        return;
                    }
                    // Authenticated
                    authAttrSet(response);
                } catch (ConfigException.Missing e) {
                    writeWrongPassErr(response);
                    return;
                } catch (Exception e) {
                    response.writeError(e.getMessage());
                    return;
                }
            }
        }

        if (msg.hasSetName()) {
            Attribute<Long> clientID = response.getContext().channel().attr(ChannelAttributes.CLIENT_ID);
            Client client = new Client();
            client.setName(msg.getClientName());
            Clients.setClient(clientID.get(), client);
        }

        List<RedisMessage> result = new ArrayList<>();
        result.add(new SimpleStringRedisMessage("server"));
        result.add(new SimpleStringRedisMessage("kronotop"));
        result.add(new SimpleStringRedisMessage("version"));

        String implementationVersion = getClass().getPackage().getImplementationVersion();
        result.add(new SimpleStringRedisMessage(implementationVersion != null ? implementationVersion : "undefined"));

        // Kronotop only supports RESP2.
        result.add(new SimpleStringRedisMessage("proto"));
        result.add(new IntegerRedisMessage(2));
        result.add(new SimpleStringRedisMessage("id"));

        Attribute<Long> clientID = response.getContext().channel().attr(ChannelAttributes.CLIENT_ID);
        result.add(new IntegerRedisMessage(clientID.get()));

        // The cluster mode is the default mode.
        result.add(new SimpleStringRedisMessage("mode"));
        result.add(new SimpleStringRedisMessage("cluster"));

        // In our design, all members of the cluster is the master of some portion of the data.
        result.add(new SimpleStringRedisMessage("role"));
        result.add(new SimpleStringRedisMessage("master"));

        // Kronotop doesn't support a module system for now
        result.add(new SimpleStringRedisMessage("modules"));
        result.add(new ArrayRedisMessage(new ArrayList<>()));
        response.writeArray(result);
    }
}
