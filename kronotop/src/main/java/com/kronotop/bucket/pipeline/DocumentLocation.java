package com.kronotop.bucket.pipeline;

import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.volume.EntryMetadata;

/**
 * Record representing document location information.
 */
public record DocumentLocation(Versionstamp documentId, int shardId, EntryMetadata entryMetadata) {
}