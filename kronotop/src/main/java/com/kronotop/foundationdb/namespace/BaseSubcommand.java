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

package com.kronotop.foundationdb.namespace;

import com.apple.foundationdb.directory.DirectoryLayer;
import com.kronotop.common.utils.DirectoryLayout;
import com.kronotop.core.Context;

import java.util.List;

class BaseSubcommand {
    final Context context;
    final DirectoryLayer directoryLayer = new DirectoryLayer(true);

    BaseSubcommand(Context context) {
        this.context = context;
    }

    DirectoryLayout getBaseSubpath() {
        return DirectoryLayout.Builder.clusterName(context.getClusterName()).namespaces();
    }

    String dottedNamespace(List<String> items) {
        return String.join(".", items);
    }
}
