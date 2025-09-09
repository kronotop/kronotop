package com.kronotop.bucket.index;

import com.apple.foundationdb.directory.DirectorySubspace;
import com.kronotop.bucket.BucketMetadata;

public record IndexEntryContainer(
        BucketMetadata metadata,
        Object indexValue,
        IndexDefinition indexDefinition,
        DirectorySubspace indexSubspace,
        int shardId,
        byte[] entryMetadata
) {
}
