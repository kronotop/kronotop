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

package com.kronotop.volume.handlers.protocol;

import com.kronotop.internal.ProtocolMessageUtil;
import com.kronotop.server.Request;

class BaseMessage {
    protected final Request request;

    public BaseMessage(Request request) {
        this.request = request;
    }

    protected byte[] readBytes(int index) {
        return ProtocolMessageUtil.readAsByteArray(request.getParams().get(index));
    }

    protected String readString(int index) {
        return ProtocolMessageUtil.readAsString(request.getParams().get(index));
    }

    protected long readLong(int index) {
        return ProtocolMessageUtil.readAsLong(request.getParams().get(index));
    }
}
