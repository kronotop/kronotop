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

package com.kronotop.redis;

import com.kronotop.server.resp.RESPErrorMessage;

public class ResolveResponse {
    private final int partId;
    private final RESPErrorMessage error;

    public ResolveResponse(RESPErrorMessage error) {
        this.partId = 0;
        this.error = error;
    }

    public ResolveResponse(int partId) {
        this.partId = partId;
        this.error = null;
    }

    public boolean hasError() {
        return error != null;
    }

    public int getPartId() {
        return partId;
    }

    public RESPErrorMessage getError() {
        return error;
    }
}
