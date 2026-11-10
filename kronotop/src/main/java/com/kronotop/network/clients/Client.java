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

package com.kronotop.network.clients;

import com.kronotop.KronotopException;
import com.kronotop.server.RESPError;

public class Client {
    private String name;

    /**
     * Checks a value that describes a client, such as its name or its library name.
     *
     * @param subject what the value is, used as the start of the error message
     * @param value   the value to check, an empty value is valid
     * @throws KronotopException if a character is outside the printable ASCII range, from '!' to '~'
     */
    public static void checkAttribute(String subject, String value) {
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (c < '!' || c > '~') {
                throw new KronotopException(String.format(RESPError.CLIENT_ATTRIBUTE_FORMAT_ERROR, subject));
            }
        }
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}
