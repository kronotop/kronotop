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

package com.kronotop.journal;

import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;

/**
 * JournalMetadata is a class that represents the metadata of a journal.
 */
public class JournalMetadata {
    private final Subspace eventsSubspace;
    private final byte[] trigger;

    public JournalMetadata(Subspace subspace) {
        this.trigger = subspace.pack("trigger");
        this.eventsSubspace = subspace.subspace(Tuple.from("events-subspace"));
    }

    public Subspace getEventsSubspace() {
        return eventsSubspace;
    }

    public byte[] getTrigger() {
        return trigger;
    }
}
