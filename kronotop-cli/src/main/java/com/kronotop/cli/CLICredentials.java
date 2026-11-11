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

package com.kronotop.cli;

import java.util.List;
import java.util.function.Supplier;

/**
 * Holds the user and the password of kronotop-cli and builds the AUTH command from them.
 */
class CLICredentials {
    static final String AUTH_ENV = "KRONOTOPCLI_AUTH";

    private final String user;
    private final String password;

    private CLICredentials(String user, String password) {
        this.user = user;
        this.password = password;
    }

    // A prompted password wins over the option, and the option wins over the environment variable.
    static CLICredentials resolve(String user, String optionPassword, String envPassword,
                                  boolean askpass, Supplier<String> prompt) {
        String password;
        if (askpass) {
            password = prompt.get();
        } else if (optionPassword != null) {
            password = optionPassword;
        } else {
            password = envPassword;
        }
        return new CLICredentials(user, password);
    }

    // Empty when there is no password. A user without a password sends nothing.
    List<String> authCommand() {
        if (password == null) {
            return List.of();
        }
        if (user == null) {
            return List.of("AUTH", password);
        }
        return List.of("AUTH", user, password);
    }
}
