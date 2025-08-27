package com.kronotop.bucket.pipeline;

public record PipelineEnv(SelectorCalculator selectorCalculator,
                          DocumentRetriever documentRetriever,
                          PredicateEvaluator predicateEvaluator,
                          CursorManager cursorManager) {
}
