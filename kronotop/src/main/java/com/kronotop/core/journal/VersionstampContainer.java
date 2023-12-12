/*
 * Copyright (c) 2023 Kronotop
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

package com.kronotop.core.journal;

import com.apple.foundationdb.tuple.Versionstamp;

import java.util.concurrent.CompletableFuture;

/**
 * The VersionstampContainer class represents a container for a Versionstamp and user version.
 * It is used to hold the versionstamp and user version of a published event.
 */
public class VersionstampContainer {
    private final CompletableFuture<byte[]> versionstamp;
    private final int userVersion;

    public VersionstampContainer(CompletableFuture<byte[]> versionstamp, int userVersion) {
        this.versionstamp = versionstamp;
        this.userVersion = userVersion;
    }

    public CompletableFuture<byte[]> getVersionstamp() {
        return versionstamp;
    }

    public int getUserVersion() {
        return userVersion;
    }

    /**
     * This method completes the Versionstamp by joining the versionstamp CompletableFuture
     * with the userVersion. It returns the completed Versionstamp.
     *
     * @return The completed Versionstamp.
     */
    public Versionstamp complete() {
        return Versionstamp.complete(versionstamp.join(), userVersion);
    }
}
