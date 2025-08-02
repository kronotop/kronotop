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

/**
 * Represents the Shards directory node in the Kronotop directory structure.
 * This class provides access to Redis and Bucket directory nodes under the "shards"
 * path of the directory layout.
 * <p>
 * The class extends KronotopDirectoryNode, inheriting basic operations such as
 * extending the directory path and converting the layout to an immutable list.
 * Upon instantiation, this class appends the "shards" identifier to the directory layout.
 */
public class Shards extends KronotopDirectoryNode {
    public Shards(List<String> layout) {
        super(layout);
        layout.add("shards");
    }

    public Redis redis() {
        return new Redis(layout);
    }

    public Bucket bucket() {
        return new Bucket(layout);
    }
}
