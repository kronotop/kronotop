package com.kronotop.bucket.handlers;

import com.apple.foundationdb.tuple.Versionstamp;

import java.nio.ByteBuffer;
import java.util.Map;

public record BucketReadResponse(int cursorId, Map<Versionstamp, ByteBuffer> entries) {
}
