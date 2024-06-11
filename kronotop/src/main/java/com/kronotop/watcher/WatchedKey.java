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

package com.kronotop.watcher;

import io.netty.channel.ChannelId;

import java.util.HashSet;
import java.util.Set;

/**
 * The WatchedKey class represents a watched key in the Watcher class.
 * It keeps track of the set of channels associated with the key and the version of the key.
 */
public class WatchedKey {
    private final Set<ChannelId> channels = new HashSet<>();
    private Long version = 0L;

    public Long getVersion() {
        return version;
    }

    public void increaseVersion() {
        version++;
    }

    public Set<ChannelId> getChannels() {
        return channels;
    }
}
