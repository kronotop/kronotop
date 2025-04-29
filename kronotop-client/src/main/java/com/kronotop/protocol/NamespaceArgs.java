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

package com.kronotop.protocol;

import io.lettuce.core.protocol.CommandArgs;

/**
 * Represents the arguments for namespace-related operations.
 * This class allows configuration of specific namespace parameters such as layer and prefix.
 * The configured arguments can be used to modify namespace behaviors or properties
 * in commands interacting with the Kronotop system.
 * <p>
 * The arguments are constructed using method chaining, enabling customization
 * based on `layer` or `prefix` settings, or both. These settings are applied
 * when building command arguments through the `build` method.
 */
public class NamespaceArgs {
    private byte[] layer;
    private byte[] prefix;

    public NamespaceArgs layer(byte[] layer) {
        this.layer = layer;
        return this;
    }

    public NamespaceArgs prefix(byte[] prefix) {
        this.prefix = prefix;
        return this;
    }

    public <K, V> void build(CommandArgs<K, V> args) {
        if (layer != null) {
            args.add(NamespaceKeywords.LAYER);
            args.add(layer);
        }

        if (prefix != null) {
            args.add(NamespaceKeywords.PREFIX);
            args.add(prefix);
        }
    }

    public static class Builder {
        private Builder() {
        }

        public static NamespaceArgs layer(byte[] layer) {
            return new NamespaceArgs().layer(layer);
        }

        public static NamespaceArgs prefix(byte[] prefix) {
            return new NamespaceArgs().prefix(prefix);
        }
    }
}
