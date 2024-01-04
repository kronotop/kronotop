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

package com.kronotop.foundationdb.zmap;

import com.kronotop.core.CommandHandlerService;
import com.kronotop.core.Context;
import com.kronotop.core.KronotopService;
import com.kronotop.server.Handlers;

public class ZMapService extends CommandHandlerService implements KronotopService {
    public static final byte SubspaceMagic = 0x01;
    public static final String NAME = "ZMap";

    public ZMapService(Context context, Handlers handlers) {
        super(context, handlers);

        registerHandler(new ZPutHandler(this));
        registerHandler(new ZGetHandler(this));
        registerHandler(new ZDelHandler(this));
        registerHandler(new ZDelRangeHandler(this));
        registerHandler(new ZDelPrefixHandler(this));
        registerHandler(new ZGetRangeHandler(this));
        registerHandler(new ZGetKeyHandler(this));
        registerHandler(new ZMutateHandler(this));
        registerHandler(new ZGetRangeSizeHandler(this));
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public Context getContext() {
        return this.context;
    }

    @Override
    public void shutdown() {
    }
}
