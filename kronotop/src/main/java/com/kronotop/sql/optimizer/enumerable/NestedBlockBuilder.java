/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.kronotop.sql.optimizer.enumerable;

import org.apache.calcite.linq4j.tree.BlockBuilder;

/**
 * Allows to build nested code blocks with tracking of current context.
 *
 * @see StrictAggImplementor#implementAdd(AggContext, AggAddContext)
 */
public interface NestedBlockBuilder {
    /**
     * Starts nested code block. The resulting block can optimize expressions
     * and reuse already calculated values from the parent blocks.
     *
     * @return new code block that can optimize expressions and reuse already
     * calculated values from the parent blocks.
     */
    BlockBuilder nestBlock();

    /**
     * Uses given block as the new code context.
     * The current block will be restored after {@link #exitBlock()} call.
     *
     * @param block new code block
     * @see #exitBlock()
     */
    void nestBlock(BlockBuilder block);

    /**
     * Returns the current code block.
     */
    BlockBuilder currentBlock();

    /**
     * Leaves the current code block.
     *
     * @see #nestBlock()
     */
    void exitBlock();
}
