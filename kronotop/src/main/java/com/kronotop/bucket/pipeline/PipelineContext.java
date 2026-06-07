/*
 * Copyright (c) 2023-2026 Burak Sezer
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.kronotop.bucket.pipeline;

import com.kronotop.bucket.bql.ast.BqlValue;
import com.kronotop.bucket.pipeline.PhysicalPlanParameterBinder.ParamBinding;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Context for pipeline execution with parameter binding support.
 * Holds the parameter binding map and parameter values for resolving Operand.Param instances.
 */
public final class PipelineContext {

    private final Map<Integer, List<ParamBinding>> bindings;
    private final List<BqlValue> parameters;

    /**
     * Creates a PipelineContext for literal mode (no parameter binding).
     * All operands will be created as Operand.Literal.
     */
    public PipelineContext() {
        this.bindings = null;
        this.parameters = null;
    }

    /**
     * Creates a PipelineContext for parameterized mode.
     *
     * @param bindings   mapping from physical node ID to parameter bindings
     * @param parameters the parameter values to use for resolving Param operands
     */
    public PipelineContext(Map<Integer, List<ParamBinding>> bindings, List<BqlValue> parameters) {
        this.bindings = bindings;
        this.parameters = parameters;
    }

    /**
     * Returns true if this context supports parameterized operands.
     */
    public boolean isParameterized() {
        return bindings != null && parameters != null;
    }

    /**
     * Creates an operand for the given physical node and occurrence.
     * In parameterized mode, creates Operand.Param; otherwise creates Operand.Literal.
     *
     * @param physicalNodeId the physical node ID
     * @param occurrence     the occurrence index (0 for single operands, 0/1 for range bounds)
     * @param literalValue   the literal value to use in non-parameterized mode
     * @return the appropriate Operand instance
     */
    public Operand createOperand(int physicalNodeId, int occurrence, BqlValue literalValue) {
        if (!isParameterized()) {
            return new Operand.Literal(literalValue);
        }

        List<ParamBinding> nodeBindings = bindings.get(physicalNodeId);
        if (nodeBindings == null) {
            // No binding found - use literal value
            return new Operand.Literal(literalValue);
        }

        for (ParamBinding binding : nodeBindings) {
            if (binding.occurrence() == occurrence) {
                return new Operand.Param(new ParamRef(binding.paramIndex()));
            }
        }

        // No binding for this occurrence - use literal value
        return new Operand.Literal(literalValue);
    }

    /**
     * Creates a list operand for the given physical node (for $in/$nin/$all).
     * In parameterized mode, creates ParamList with references to each parameter;
     * otherwise creates LiteralList.
     *
     * @param physicalNodeId the physical node ID
     * @param literalValues  the literal values (used in non-parameterized mode)
     * @return the appropriate list operand
     */
    public Operand createListOperand(int physicalNodeId, List<BqlValue> literalValues) {
        if (!isParameterized()) {
            return new Operand.LiteralList(literalValues);
        }

        List<ParamBinding> nodeBindings = bindings.get(physicalNodeId);
        if (nodeBindings == null || nodeBindings.isEmpty()) {
            // No binding found - use literal values
            return new Operand.LiteralList(literalValues);
        }

        // Create ParamRef for each binding, sorted by occurrence
        List<ParamRef> refs = new ArrayList<>(nodeBindings.size());
        for (ParamBinding binding : nodeBindings) {
            refs.add(new ParamRef(binding.paramIndex()));
        }
        return new Operand.ParamList(refs);
    }

    /**
     * Returns the parameter list for resolving Param operands.
     */
    public List<BqlValue> getParameters() {
        return parameters;
    }
}
