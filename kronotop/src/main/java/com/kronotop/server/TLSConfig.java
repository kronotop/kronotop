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

package com.kronotop.server;

import com.typesafe.config.Config;

import java.io.File;

/**
 * TLS configuration for RESP servers and internal cluster clients.
 *
 * @param enabled  whether TLS is active
 * @param certFile server certificate chain (PEM)
 * @param keyFile  server private key (PEM)
 * @param caFile   optional CA certificate for trust store (maybe null)
 */
public record TLSConfig(boolean enabled, File certFile, File keyFile, File caFile) {

    private static final TLSConfig DISABLED = new TLSConfig(false, null, null, null);

    /**
     * Returns a disabled TLS configuration.
     */
    public static TLSConfig disabled() {
        return DISABLED;
    }

    /**
     * Reads TLS configuration from the application config for the given server kind.
     * Returns a disabled config if the tls block is absent or {@code tls.enabled} is false.
     */
    public static TLSConfig fromConfig(Config config, ServerKind kind) {
        String prefix = String.format("network.%s.tls", kind.toString().toLowerCase());

        if (!config.hasPath(prefix + ".enabled") || !config.getBoolean(prefix + ".enabled")) {
            return DISABLED;
        }

        File certFile = new File(config.getString(prefix + ".cert_path"));
        File keyFile = new File(config.getString(prefix + ".key_path"));

        File caFile = null;
        if (config.hasPath(prefix + ".ca_path")) {
            caFile = new File(config.getString(prefix + ".ca_path"));
        }

        return new TLSConfig(true, certFile, keyFile, caFile);
    }
}
