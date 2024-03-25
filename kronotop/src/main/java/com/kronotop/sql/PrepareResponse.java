package com.kronotop.sql;

import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.core.TransactionUtils;
import com.kronotop.core.VersionstampUtils;
import com.kronotop.server.Response;
import com.kronotop.server.resp3.*;
import com.kronotop.sql.executor.PlanContext;
import com.kronotop.sql.executor.Row;
import io.netty.buffer.ByteBuf;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexLiteral;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

/**
 * The PrepareResponse class contains a static method for preparing a RedisMessage based on a PlanContext.
 */
public class PrepareResponse {

    private static RedisMessage prepareMutationResponse(PlanContext planContext) {
        RelOptTable table = planContext.getTable();

        if (planContext.getSqlMessage().getReturning().isEmpty()) {
            // The returning set is empty. Return OK.
            return new SimpleStringRedisMessage(Response.OK);
        }

        Map<RedisMessage, RedisMessage> children = new HashMap<>();
        for (Row<RexLiteral> row : planContext.getRexLiterals()) {
            assert table != null;
            for (RelDataTypeField field : table.getRowType().getFieldList()) {
                if (!planContext.getSqlMessage().getReturning().contains(field.getName())) {
                    continue;
                }
                RexLiteral rexLiteral = row.get(field.getName());
                if (rexLiteral == null) {
                    children.put(new SimpleStringRedisMessage(field.getName()), NullRedisMessage.INSTANCE);
                    continue;
                }

                if (field.getName().equals("id")) {
                    if (TransactionUtils.getAutoCommit(planContext.getChannelContext())) {
                        byte[] versionBytes = planContext.getVersionstamp().join();
                        Versionstamp id = Versionstamp.complete(versionBytes, planContext.getUserVersion());

                        ByteBuf buf = planContext.getChannelContext().alloc().buffer();
                        buf.writeBytes(VersionstampUtils.base64Encode(id).getBytes());
                        children.put(new SimpleStringRedisMessage(field.getName()), new FullBulkStringRedisMessage(buf));
                    } else {
                        LinkedList<Integer> asyncReturning = planContext.getAsyncReturning();
                        Integer currentUserVersion = planContext.getUserVersion();
                        asyncReturning.add(currentUserVersion);

                        ByteBuf buf = planContext.getChannelContext().alloc().buffer();
                        buf.writeBytes(String.format("$%d", currentUserVersion).getBytes());
                        children.put(new SimpleStringRedisMessage(field.getName()), new FullBulkStringRedisMessage(buf));
                    }
                    continue;
                }

                switch (rexLiteral.getType().getSqlTypeName()) {
                    case TINYINT -> {
                        Long value = rexLiteral.getValueAs(Long.class);
                        assert value != null;
                        children.put(new SimpleStringRedisMessage(field.getName()), new IntegerRedisMessage(value));
                    }
                    case INTEGER -> {
                        Integer value = rexLiteral.getValueAs(Integer.class);
                        assert value != null;
                        children.put(new SimpleStringRedisMessage(field.getName()), new IntegerRedisMessage(value));
                    }
                    case CHAR -> {
                        String value = rexLiteral.getValueAs(String.class);
                        assert value != null;
                        ByteBuf buf = planContext.getChannelContext().alloc().buffer();
                        buf.writeBytes(value.getBytes());
                        children.put(new SimpleStringRedisMessage(field.getName()), new FullBulkStringRedisMessage(buf));
                    }
                }
            }
        }
        return new MapRedisMessage(children);
    }

    public static RedisMessage prepare(PlanContext planContext) {
        if (planContext.getMutated()) {
            return prepareMutationResponse(planContext);
        }

        // For everything else!
        return new SimpleStringRedisMessage(Response.OK);
    }
}
