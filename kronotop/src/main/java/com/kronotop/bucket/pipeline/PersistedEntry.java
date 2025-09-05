package com.kronotop.bucket.pipeline;

import java.nio.ByteBuffer;

public record PersistedEntry(int shardId, ByteBuffer document) {
}
