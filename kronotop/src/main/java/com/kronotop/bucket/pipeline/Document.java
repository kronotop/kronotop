package com.kronotop.bucket.pipeline;

import com.apple.foundationdb.tuple.Versionstamp;

import java.nio.ByteBuffer;

public record Document(Versionstamp documentId, ByteBuffer body) {
}
