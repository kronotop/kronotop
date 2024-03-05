package com.kronotop.sql.plan;

import org.apache.calcite.plan.*;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.metadata.Metadata;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.util.Litmus;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class MockRelNode implements RelNode {
    private final String relTypeName;
    private final List<RelNode> inputs;

    public MockRelNode(String relTypeName) {
        this(relTypeName, new ArrayList<>());
    }

    public MockRelNode(String relTypeName, List<RelNode> inputs) {
        this.relTypeName = relTypeName;
        this.inputs = inputs;
    }

    public void addInput(RelNode relNode) {
        this.inputs.add(relNode);
    }

    @Override
    public String toString() {
        return relTypeName;
    }

    @Override
    public @Nullable Convention getConvention() {
        return null;
    }

    @Override
    public @Nullable String getCorrelVariable() {
        return null;
    }

    @Override
    public RelNode getInput(int i) {
        return inputs.get(i);
    }

    @Override
    public int getId() {
        return 0;
    }

    @Override
    public RelTraitSet getTraitSet() {
        return null;
    }

    @Override
    public RelDataType getRowType() {
        return null;
    }

    @Override
    public String getDescription() {
        return null;
    }

    @Override
    public RelDataType getExpectedInputRowType(int ordinalInParent) {
        return null;
    }

    @Override
    public List<RelNode> getInputs() {
        return inputs;
    }

    @Override
    public RelOptCluster getCluster() {
        return null;
    }

    @Override
    public double estimateRowCount(RelMetadataQuery mq) {
        return 0;
    }

    @Override
    public Set<CorrelationId> getVariablesSet() {
        return null;
    }

    @Override
    public void collectVariablesUsed(Set<CorrelationId> variableSet) {

    }

    @Override
    public void collectVariablesSet(Set<CorrelationId> variableSet) {

    }

    @Override
    public void childrenAccept(RelVisitor visitor) {

    }

    @Override
    public @Nullable RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        return null;
    }

    @Override
    public <M extends Metadata> M metadata(Class<M> metadataClass, RelMetadataQuery mq) {
        return null;
    }

    @Override
    public void explain(RelWriter pw) {

    }

    @Override
    public RelNode onRegister(RelOptPlanner planner) {
        return null;
    }

    @Override
    public RelDigest getRelDigest() {
        return null;
    }

    @Override
    public void recomputeDigest() {

    }

    @Override
    public boolean deepEquals(@Nullable Object obj) {
        return false;
    }

    @Override
    public int deepHashCode() {
        return 0;
    }

    @Override
    public void replaceInput(int ordinalInParent, RelNode p) {

    }

    @Override
    public @Nullable RelOptTable getTable() {
        return null;
    }

    @Override
    public String getRelTypeName() {
        return relTypeName;
    }

    @Override
    public boolean isValid(Litmus litmus, Context context) {
        return false;
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return null;
    }

    @Override
    public void register(RelOptPlanner planner) {

    }

    @Override
    public RelNode accept(RelShuttle shuttle) {
        return null;
    }

    @Override
    public RelNode accept(RexShuttle shuttle) {
        return null;
    }
}
