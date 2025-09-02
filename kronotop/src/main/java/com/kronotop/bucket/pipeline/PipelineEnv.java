package com.kronotop.bucket.pipeline;

public record PipelineEnv(DocumentRetriever documentRetriever, CursorManager cursorManager) {
}
