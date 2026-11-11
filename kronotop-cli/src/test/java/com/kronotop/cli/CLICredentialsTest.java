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

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;

class CLICredentialsTest {

    private static final Supplier<String> NO_PROMPT = () -> {
        throw new AssertionError("the password prompt must not be used");
    };

    @Test
    void shouldPreferPromptedPasswordWhenAskpassIsSet() {
        // Behavior: with askpass set, the password typed at the prompt is used, and both the option password
        // and the environment variable password are ignored.
        CLICredentials credentials = CLICredentials.resolve(null, "from-option", "from-env", true, () -> "from-prompt");

        assertEquals(List.of("AUTH", "from-prompt"), credentials.authCommand());
    }

    @Test
    void shouldPreferOptionPasswordOverEnvironmentVariable() {
        // Behavior: without askpass, a password given as an option wins over the environment variable password,
        // and the prompt is not shown.
        CLICredentials credentials = CLICredentials.resolve(null, "from-option", "from-env", false, NO_PROMPT);

        assertEquals(List.of("AUTH", "from-option"), credentials.authCommand());
    }

    @Test
    void shouldUseEnvironmentVariableWhenNoOptionPasswordIsGiven() {
        // Behavior: without askpass and without an option password, the environment variable password is used.
        CLICredentials credentials = CLICredentials.resolve(null, null, "from-env", false, NO_PROMPT);

        assertEquals(List.of("AUTH", "from-env"), credentials.authCommand());
    }

    @Test
    void shouldBuildAuthCommandWithUserAndPassword() {
        // Behavior: with a user and a password, the AUTH command carries the user first and the password second.
        CLICredentials credentials = CLICredentials.resolve("admin", "secret", null, false, NO_PROMPT);

        assertEquals(List.of("AUTH", "admin", "secret"), credentials.authCommand());
    }

    @Test
    void shouldBuildNoAuthCommandWithoutPassword() {
        // Behavior: a user without any password source produces no AUTH command, so nothing is sent.
        CLICredentials credentials = CLICredentials.resolve("admin", null, null, false, NO_PROMPT);

        assertEquals(List.of(), credentials.authCommand());
    }
}
