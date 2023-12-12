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

import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;

/**
 * The JournalMetadata class represents the metadata of a journal.
 * It encapsulates the subspace, index key, and journal key related to the journal.
 */
public class JournalMetadata {
    private final Subspace subspace;
    private final byte[] indexKey;
    private final byte[] journalKey;

    public JournalMetadata(String journal, Subspace subspace) {
        this.subspace = subspace;
        this.journalKey = this.subspace.subspace(Tuple.from(journal)).pack();
        this.indexKey = this.subspace.subspace(Tuple.from(String.format("%s-index", journal))).pack();
    }

    public byte[] getIndexKey() {
        return indexKey;
    }

    public byte[] getJournalKey() {
        return journalKey;
    }

    public Subspace getSubspace() {
        return subspace;
    }
}
