/*
 * Copyright (c) 2023-2025 Burak Sezer
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.kronotop.volume;

import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Versionstamp;

import java.nio.ByteBuffer;
import java.util.Arrays;

import static com.kronotop.volume.Subspaces.MUTATION_TRIGGER;

public class VolumeUtil {
    public static byte[] computeMutationTriggerKey(DirectorySubspace subspace) {
        return subspace.pack(MUTATION_TRIGGER);
    }

    public static Versionstamp extractVersionstampFromValue(byte[] value) {
        byte[] trVersion = Arrays.copyOfRange(value, 0, 10);
        int userVersion = ByteBuffer.wrap(Arrays.copyOfRange(value, 11, 13)).getShort();
        return Versionstamp.complete(trVersion, userVersion);
    }
}
