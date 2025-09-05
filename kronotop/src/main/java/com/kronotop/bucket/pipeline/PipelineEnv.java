package com.kronotop.bucket.pipeline;

import com.kronotop.bucket.BucketService;

public record PipelineEnv(
        BucketService bucketService,
        DocumentRetriever documentRetriever,
        CursorManager cursorManager
) {
}
