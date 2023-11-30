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

package com.kronotop.core.cluster.journal;

import com.apple.foundationdb.MutationType;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.tuple.Tuple;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kronotop.common.utils.ByteUtils;
import com.kronotop.core.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Publisher extends BaseJournal {
    private static final Logger LOGGER = LoggerFactory.getLogger(Publisher.class);

    public Publisher(Context context) {
        super(context);
    }

    private long publishInternal(Transaction tr, String journal, Object event) {
        try {
            JournalMetadata journalMetadata = cache.get(journal);
            byte[] data = new ObjectMapper().writeValueAsBytes(event);

            long offset = lastIndex(tr, journalMetadata);
            tr.set(journalMetadata.getSubspace().subspace(Tuple.from(offset, "K")).pack(), data);
            tr.mutate(MutationType.ADD, journalMetadata.getJournalKey(), ByteUtils.fromLong(1L));

            return offset;
        } catch (Exception e) {
            LOGGER.error("Failed to publish event: {}", e.getMessage());
            // Will be retried
            throw new RuntimeException(e);
        }
    }

    public long publish(Transaction tr, String journal, Object event) {
        return publishInternal(tr, journal, event);
    }

    public long publish(String journal, Object event) {
        return context.getFoundationDB().run((Transaction tr) -> publishInternal(tr, journal, event));
    }
}
