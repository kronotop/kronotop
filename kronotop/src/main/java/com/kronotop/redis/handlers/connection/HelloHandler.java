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

package com.kronotop.redis.handlers.connection;

import com.kronotop.instance.KronotopInstance;
import com.kronotop.network.clients.Client;
import com.kronotop.network.clients.Clients;
import com.kronotop.redis.RedisService;
import com.kronotop.redis.handlers.BaseHandler;
import com.kronotop.redis.handlers.connection.protocol.HelloMessage;
import com.kronotop.server.*;
import com.kronotop.server.annotation.Command;
import com.kronotop.server.resp3.*;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.util.Attribute;

import java.util.*;


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
                getCtx().
                channel().
                attr(SessionAttributes.AUTH);
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
            Attribute<Long> clientID = response.getCtx().channel().attr(SessionAttributes.CLIENT_ID);
            Client client = new Client();
            client.setName(msg.getClientName());
            Clients.setClient(clientID.get(), client);
        }

        if (msg.getProtover().equals(RESPVersion.RESP2.getValue())) {
            resp2Response(response);
            request.getSession().setProtocolVersion(RESPVersion.RESP2);
        } else if (msg.getProtover().equals(RESPVersion.RESP3.getValue())) {
            resp3Response(response);
            request.getSession().setProtocolVersion(RESPVersion.RESP3);
        } else {
            // Actually, this case was already handled by the message parser but safety is a good thing.
            throw new NoProtoException();
        }
    }

    private SimpleStringRedisMessage getVersion2() {
        String implementationVersion = getClass().getPackage().getImplementationVersion();
        return new SimpleStringRedisMessage(implementationVersion != null ? implementationVersion : "undefined");
    }

    private FullBulkStringRedisMessage getVersion3() {
        String implementationVersion = getClass().getPackage().getImplementationVersion();
        return makeFullBulkString(implementationVersion != null ? implementationVersion : "undefined");
    }

    private FullBulkStringRedisMessage makeFullBulkString(String content) {
        return new FullBulkStringRedisMessage(PooledByteBufAllocator.DEFAULT.buffer(content.length()).writeBytes(content.getBytes()));
    }

    private void resp3Response(Response response) {
        // We want to keep the insertion order.
        Map<RedisMessage, RedisMessage> map = new LinkedHashMap<>();
        map.put(makeFullBulkString("server"), makeFullBulkString(KronotopInstance.KING_OF_THE_DATABASES));
        map.put(makeFullBulkString("version"), getVersion3());
        map.put(makeFullBulkString("proto"), new IntegerRedisMessage(RESPVersion.RESP3.getValue()));

        Attribute<Long> clientID = response.getCtx().channel().attr(SessionAttributes.CLIENT_ID);
        map.put(makeFullBulkString("id"), new IntegerRedisMessage(clientID.get()));
        map.put(makeFullBulkString("mode"), makeFullBulkString("cluster"));
        map.put(makeFullBulkString("role"), makeFullBulkString("master"));
        map.put(makeFullBulkString("modules"), new ArrayRedisMessage(new ArrayList<>()));
        response.writeMap(map);
    }

    private void resp2Response(Response response) {
        List<RedisMessage> result = new ArrayList<>();
        result.add(new SimpleStringRedisMessage("server"));
        result.add(new SimpleStringRedisMessage(KronotopInstance.KING_OF_THE_DATABASES));

        result.add(new SimpleStringRedisMessage("version"));
        result.add(getVersion2());

        result.add(new SimpleStringRedisMessage("proto"));
        result.add(new IntegerRedisMessage(RESPVersion.RESP2.getValue()));
        result.add(new SimpleStringRedisMessage("id"));

        Attribute<Long> clientID = response.getCtx().channel().attr(SessionAttributes.CLIENT_ID);
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
