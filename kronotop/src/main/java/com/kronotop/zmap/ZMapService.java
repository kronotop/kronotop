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

package com.kronotop.zmap;

import com.kronotop.CommandHandlerService;
import com.kronotop.Context;
import com.kronotop.KronotopService;
import com.kronotop.server.ServerKind;
import com.kronotop.zmap.handlers.*;

/**
 * Service that registers all ZMap command handlers. ZMap is an ordered key-value data structure
 * backed by FoundationDB.
 */
public class ZMapService extends CommandHandlerService implements KronotopService {
    public static final String NAME = "ZMap";

    public ZMapService(Context context) {
        super(context, NAME);

        handlerMethod(ServerKind.EXTERNAL, new ZSetHandler(this));
        handlerMethod(ServerKind.EXTERNAL, new ZGetHandler(this));
        handlerMethod(ServerKind.EXTERNAL, new ZDelHandler(this));
        handlerMethod(ServerKind.EXTERNAL, new ZDelRangeHandler(this));
        handlerMethod(ServerKind.EXTERNAL, new ZGetRangeHandler(this));
        handlerMethod(ServerKind.EXTERNAL, new ZGetKeyHandler(this));
        handlerMethod(ServerKind.EXTERNAL, new ZMutateHandler(this));
        handlerMethod(ServerKind.EXTERNAL, new ZGetRangeSizeHandler(this));
        handlerMethod(ServerKind.EXTERNAL, new ZIncI64Handler(this));
        handlerMethod(ServerKind.EXTERNAL, new ZGetI64Handler(this));
        handlerMethod(ServerKind.EXTERNAL, new ZIncF64Handler(this));
        handlerMethod(ServerKind.EXTERNAL, new ZGetF64Handler(this));
        handlerMethod(ServerKind.EXTERNAL, new ZIncD128Handler(this));
        handlerMethod(ServerKind.EXTERNAL, new ZGetD128Handler(this));
        handlerMethod(ServerKind.EXTERNAL, new ZSetI64Handler(this));
        handlerMethod(ServerKind.EXTERNAL, new ZSetF64Handler(this));
        handlerMethod(ServerKind.EXTERNAL, new ZSetD128Handler(this));
    }
}
