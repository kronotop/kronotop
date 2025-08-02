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

package com.kronotop.bucket.handlers;

import com.kronotop.bucket.BucketService;
import com.kronotop.bucket.handlers.protocol.QueryMessage;
import com.kronotop.server.annotation.Command;
import com.kronotop.server.annotation.MinimumParameterCount;

// Alias for BUCKET.QUERY

@Command(QueryMessage.COMMAND)
@MinimumParameterCount(QueryMessage.MINIMUM_PARAMETER_COUNT)
public class QueryHandler extends BucketQueryHandler {
    public QueryHandler(BucketService service) {
        super(service);
    }
}
