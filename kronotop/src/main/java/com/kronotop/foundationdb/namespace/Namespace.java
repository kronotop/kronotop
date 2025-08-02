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

package com.kronotop.foundationdb.namespace;

import com.apple.foundationdb.directory.DirectorySubspace;
import com.kronotop.DataStructureKind;

import java.util.Optional;

public class Namespace {
    public static String INTERNAL_LEAF = "__internal__";

    private DirectorySubspace zmap;
    private DirectorySubspace bucket;

    public Optional<DirectorySubspace> get(DataStructureKind kind) {
        return switch (kind) {
            case ZMAP -> Optional.ofNullable(zmap);
            case BUCKET -> Optional.ofNullable(bucket);
        };
    }

    public void set(DataStructureKind kind, DirectorySubspace subspace) {
        switch (kind) {
            case ZMAP -> zmap = subspace;
            case BUCKET -> bucket = subspace;
        }
    }
}
