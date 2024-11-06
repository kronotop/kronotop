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

package com.kronotop.directory;

import java.util.List;

public class Metadata extends KronotopDirectoryNode {

    Metadata(List<String> layout) {
        super(layout);
        layout.add("metadata");
    }

    public Shards shards() {
        return new Shards(layout);
    }

    public Volumes volumes() {
        return new Volumes(layout);
    }

    public Members members() {
        return new Members(layout);
    }
}
