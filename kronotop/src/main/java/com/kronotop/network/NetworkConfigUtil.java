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

package com.kronotop.network;

import com.kronotop.MissingConfigException;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;

import java.util.List;

public class NetworkConfigUtil {

    /**
     * Reads the bind host of the given network interface from the configuration.
     */
    public static String getHost(Config config, String networkInterface) {
        String configPath = String.format("network.%s.host", networkInterface);
        try {
            return config.getString(configPath);
        } catch (ConfigException.Missing exp) {
            throw new MissingConfigException(configPath + " is mandatory");
        } catch (ConfigException.WrongType exp) {
            throw new MissingConfigException(configPath + " must be a string");
        }
    }

    /**
     * Reads the bind port of the given network interface from the configuration.
     */
    public static int getPort(Config config, String networkInterface) {
        String configPath = String.format("network.%s.port", networkInterface);
        try {
            return config.getInt(configPath);
        } catch (ConfigException.Missing exp) {
            throw new MissingConfigException(configPath + " is mandatory");
        } catch (ConfigException.WrongType exp) {
            throw new MissingConfigException(configPath + " must be an integer");
        }
    }

    /**
     * Reads the advertised addresses of the given network interface from the configuration.
     */
    public static List<String> getAdvertise(Config config, String networkInterface) {
        String configPath = String.format("network.%s.advertise", networkInterface);
        try {
            return config.getStringList(configPath);
        } catch (ConfigException.Missing exp) {
            throw new MissingConfigException(configPath + " is mandatory");
        } catch (ConfigException.WrongType exp) {
            throw new MissingConfigException(configPath + " must be a list of strings");
        }
    }
}