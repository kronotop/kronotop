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

package com.kronotop.directory;

import java.util.List;

public class Tasks extends KronotopDirectoryNode {

    public Tasks(List<String> layout) {
        super(layout);
        layout.add("tasks");
    }

    public Task task(String name) {
        return new Task(layout, name);
    }

    public static class Task extends KronotopDirectoryNode {
        public Task(List<String> layout, String name) {
            super(layout);
            layout.add(name);
        }
    }
}
