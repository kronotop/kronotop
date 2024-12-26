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

package com.kronotop.volume.handlers;

import com.kronotop.Context;
import com.kronotop.volume.VolumeService;
import io.netty.buffer.ByteBuf;

public class BaseHandler {
    protected VolumeService service;
    protected Context context;

    public BaseHandler(VolumeService service) {
        this.service = service;
        this.context = service.getContext();
    }

    /**
     * Reads the content of the provided ByteBuf as a string.
     *
     * @param buf the ByteBuf containing the raw bytes to be read
     * @return a string representation of the bytes in the provided ByteBuf
     */
    protected String readAsString(ByteBuf buf) {
        byte[] raw = new byte[buf.readableBytes()];
        buf.readBytes(raw);
        return new String(raw);
    }

}
