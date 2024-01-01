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

package com.kronotop.server;

import com.kronotop.server.resp3.FullBulkStringRedisMessage;
import com.kronotop.server.resp3.RedisMessage;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;

import java.util.List;

public interface Response {
    void writeOK();

    void writeQUEUED();

    void writeInteger(long value);

    void writeArray(List<RedisMessage> children);

    void writeSimpleString(String msg);

    void writeError(String content);

    <T> void writeError(T prefix, String content);

    void write(ByteBuf content);

    void writeFullBulkString(FullBulkStringRedisMessage msg);

    void flush();

    ChannelHandlerContext getContext();
}
