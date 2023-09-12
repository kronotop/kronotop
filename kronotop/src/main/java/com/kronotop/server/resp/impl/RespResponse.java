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

package com.kronotop.server.resp.impl;

import com.kronotop.common.resp.RESPError;
import com.kronotop.server.resp.Response;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.redis.*;

import java.util.List;

public class RespResponse implements Response {
    private final ChannelHandlerContext ctx;

    public RespResponse(ChannelHandlerContext ctx) {
        this.ctx = ctx;
    }

    @Override
    public void writeOK() {
        writeSimpleString("OK");
    }

    @Override
    public void writeQUEUED() {
        ctx.write(new SimpleStringRedisMessage("QUEUED"));
    }

    @Override
    public void writeInteger(long value) {
        ctx.writeAndFlush(new IntegerRedisMessage(value));
    }

    @Override
    public void writeArray(List<RedisMessage> children) {
        ctx.writeAndFlush(new ArrayRedisMessage(children));
    }

    @Override
    public void writeSimpleString(String msg) {
        ctx.writeAndFlush(new SimpleStringRedisMessage(msg));
    }

    @Override
    public void write(ByteBuf content) {
        ctx.writeAndFlush(new FullBulkStringRedisMessage(content));
    }

    @Override
    public void writeFullBulkString(FullBulkStringRedisMessage msg) {
        ctx.writeAndFlush(msg);
    }

    @Override
    public ChannelHandlerContext getContext() {
        return this.ctx;
    }

    @Override
    public void writeError(String content) {
        this.writeError(RESPError.ERR, content);
    }

    @Override
    public <T> void writeError(T prefix, String content) {
        ctx.writeAndFlush(new ErrorRedisMessage(String.format("%s %s", prefix, content)));
    }

    @Override
    public void flush() {
        ctx.flush();
    }
}
