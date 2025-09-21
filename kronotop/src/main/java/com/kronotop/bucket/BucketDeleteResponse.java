package com.kronotop.bucket;

import com.apple.foundationdb.tuple.Versionstamp;

import java.util.List;

public record BucketDeleteResponse(int cursorId, List<Versionstamp> versionstamps) {
}
