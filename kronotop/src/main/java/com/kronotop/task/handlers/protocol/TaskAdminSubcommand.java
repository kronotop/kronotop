/*
 * Copyright (c) 2023-2024 Kronotop
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

package com.kronotop.task.  handlers.protocol;

public enum TaskAdminSubcommand {
    LIST("list");

    private final String value;

    TaskAdminSubcommand(String value) {
        this.value = value;
    }

    public static TaskAdminSubcommand valueOfSubcommand(String command) {
        for (TaskAdminSubcommand value : values()) {
            if (value.value.equalsIgnoreCase(command)) {
                return value;
            }
        }
        throw new IllegalArgumentException();
    }

    public String getValue() {
        return value;
    }

    @Override
    public String toString() {
        return String.valueOf(value);
    }
}
