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

package com.kronotop.volume.handlers.protocol;

import com.kronotop.KronotopException;
import com.kronotop.server.RESPError;
import com.kronotop.server.Request;
import io.netty.buffer.ByteBuf;

class BaseMessage {
    protected final Request request;

    public BaseMessage(Request request) {
        this.request = request;
    }

    protected byte[] readBytes(int index) {
        ByteBuf byteBuf = request.getParams().get(index);
        byte[] data = new byte[byteBuf.readableBytes()];
        byteBuf.readBytes(data);
        return data;
    }

    protected String readString(int index) {
        return new String(readBytes(index));
    }

    protected long readLong(int index) {
        String data = readString(index);
        try {
            return Long.parseLong(data);
        } catch (NumberFormatException e) {
            throw new KronotopException(RESPError.NUMBER_FORMAT_EXCEPTION_MESSAGE_LONG);
        }
    }
}
