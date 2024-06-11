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


package com.kronotop.volume;

import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.Context;

import java.util.List;

public class Volume {
    private final Context context;
    private final DirectorySubspace subspace;

    public Volume(Context context, DirectorySubspace subspace) {
        this.context = context;
        this.subspace = subspace;
    }

    public List<Versionstamp> save(List<byte[]> values) {
        // 1- Open the latest segment
        // 2- Save the entries
        // 3- Associate offsets and entries
        // 4- Save metadata to FDB using version timestamps.
        return null;
    }
}
