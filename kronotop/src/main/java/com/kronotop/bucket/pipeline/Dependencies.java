package com.kronotop.bucket.pipeline;

public record Dependencies(SelectorCalculator selectorCalculator,
                           DocumentRetriever documentRetriever,
                           FilterEvaluator filterEvaluator,
                           CursorManager cursorManager) {
}
