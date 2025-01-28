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

public class Namespaces extends KronotopDirectoryNode {
    public Namespaces(List<String> layout) {
        super(layout);
        layout.add("namespaces");
    }

    public Namespace namespace(String namespace) {
        return new Namespace(layout, namespace);
    }

    public Namespace namespace(List<String> subpath) {
        return new Namespace(layout, subpath);
    }

    public static class Namespace extends KronotopDirectoryNode {
        public Namespace(List<String> layout, String namespace) {
            super(layout);
            layout.add(namespace);
        }

        public Namespace(List<String> layout, List<String> subpath) {
            super(layout);
            layout.addAll(subpath);
        }
    }
}
