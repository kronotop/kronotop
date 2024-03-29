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

package com.kronotop.sql;

import com.kronotop.server.Request;
import com.kronotop.server.Response;

public class ExecutionContext {
    private final Request request;
    private final Response response;
    private String schema;

    public ExecutionContext(Request request, Response response) {
        this.request = request;
        this.response = response;
    }

    public String getSchema() {
        // TODO: Remove this
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public Response getResponse() {
        return response;
    }

    public Request getRequest() {
        return request;
    }
}
