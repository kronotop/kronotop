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

import com.apple.foundationdb.directory.DirectorySubspace;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;

public class KronotopDirectoryNode {
    protected final List<String> layout;

    public KronotopDirectoryNode(List<String> layout) {
        this.layout = layout;
    }

    public List<String> extend(String e) {
        layout.add(e);
        return toList();
    }

    public List<String> extend(List<String> subpath) {
        layout.addAll(subpath);
        return toList();
    }

    public List<String> toList() {
        return ImmutableList.copyOf(layout);
    }

    public List<String> excludeSubspace(DirectorySubspace subspace) {
        ArrayList<String> newLayout = new ArrayList<>();
        for (int i = 0; i <= layout.size() - 1; i++) {
            if (i > subspace.getPath().size() - 1) {
                newLayout.add(layout.get(i));
            }
        }
        return newLayout;
    }

    @Override
    public String toString() {
        return String.join(".", layout);
    }
}
