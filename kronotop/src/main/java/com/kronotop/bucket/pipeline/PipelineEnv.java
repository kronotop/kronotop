package com.kronotop.bucket.pipeline;

public record PipelineEnv(SelectorCalculator selectorCalculator,
                          DocumentRetriever documentRetriever,
                          CursorManager cursorManager) {
}
