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

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.tuple.Tuple;
import com.kronotop.common.KronotopException;
import com.kronotop.common.utils.ByteUtils;
import com.kronotop.core.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutionException;

public class Consumer extends BaseJournal {
    private static final Logger LOGGER = LoggerFactory.getLogger(Consumer.class);

    public Consumer(Context context) {
        super(context);
    }

    public Event consumeEvent(Transaction tr, String journal, long offset) {
        try {
            JournalMetadata journalMetadata = cache.get(journal);
            final KeyValue item = firstItem(tr, journalMetadata, offset);
            if (item == null)
                return null;

            long itemOffset = (long) Tuple.fromBytes(item.getKey()).get(2);
            return new Event(itemOffset, item.getValue());
        } catch (ExecutionException e) {
            LOGGER.error("Failed to consume event: {}", e.getMessage());
            throw new KronotopException(e.getCause());
        }
    }

    public Event consumeEvent(String journal, long offset) {
        return context.getFoundationDB().run(tr -> consumeEvent(tr, journal, offset));
    }

    public long getLatestIndex(String journal) {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            JournalMetadata journalMetadata = cache.get(journal);
            byte[] value = tr.snapshot().get(journalMetadata.getIndexKey()).join();
            if (value == null)
                return 0;

            return ByteUtils.toLong(value);
        } catch (ExecutionException e) {
            LOGGER.error("Failed to get latest index: {}", e.getMessage());
            throw new KronotopException(e.getCause());
        }
    }
}