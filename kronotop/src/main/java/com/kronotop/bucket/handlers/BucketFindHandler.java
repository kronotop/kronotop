// Copyright (C) 2025 Burak Sezer
// Use of this software is governed by the Business Source License included
// in the LICENSE.TXT file and at www.mariadb.com/bsl11.

// Change Date: 5 years after release

// On the date above, in accordance with the Business Source License,
// use of this software will be governed by the open source license specified
// in the LICENSE.TXT file.

package com.kronotop.bucket.handlers;

import com.apple.foundationdb.Transaction;
import com.kronotop.bucket.BucketPrefix;
import com.kronotop.bucket.BucketService;
import com.kronotop.bucket.BucketSubspace;
import com.kronotop.bucket.BucketSubspaceUtils;
import com.kronotop.bucket.executor.PlanExecutor;
import com.kronotop.bucket.handlers.protocol.BucketFindMessage;
import com.kronotop.bucket.planner.physical.PhysicalNode;
import com.kronotop.internal.TransactionUtils;
import com.kronotop.server.Handler;
import com.kronotop.server.MessageTypes;
import com.kronotop.server.Request;
import com.kronotop.server.Response;
import com.kronotop.server.annotation.Command;
import com.kronotop.server.annotation.MaximumParameterCount;
import com.kronotop.server.annotation.MinimumParameterCount;
import com.kronotop.server.resp3.FullBulkStringRedisMessage;
import com.kronotop.server.resp3.RedisMessage;
import com.kronotop.volume.Prefix;
import io.netty.buffer.ByteBuf;

import java.util.LinkedList;
import java.util.List;

@Command(BucketFindMessage.COMMAND)
@MaximumParameterCount(BucketFindMessage.MAXIMUM_PARAMETER_COUNT)
@MinimumParameterCount(BucketFindMessage.MINIMUM_PARAMETER_COUNT)
public class BucketFindHandler extends BaseBucketHandler implements Handler {

    public BucketFindHandler(BucketService service) {
        super(service);
    }

    @Override
    public void beforeExecute(Request request) {
        request.attr(MessageTypes.BUCKETFIND).set(new BucketFindMessage(request));
    }

    @Override
    public void execute(Request request, Response response) throws Exception {
        BucketFindMessage message = request.attr(MessageTypes.BUCKETFIND).get();

        Transaction tr = TransactionUtils.getOrCreateTransaction(service.getContext(), request.getSession());
        BucketSubspace subspace = BucketSubspaceUtils.open(context, request.getSession(), tr);

        Prefix prefix = BucketPrefix.getOrSetBucketPrefix(context, tr, subspace, message.getBucket());

        PhysicalNode plan = service.getPlanner().plan(message.getBucket(), message.getQuery());
        PlanExecutor executor = new PlanExecutor(context, plan);

        List<byte[]> entries = executor.execute();
        if (entries == null || entries.isEmpty()) {
            response.writeArray(List.of());
            return;
        }
        List<RedisMessage> children = new LinkedList<>();
        for (byte[] entry : entries) {
            ByteBuf buf = response.getCtx().alloc().buffer();
            buf.writeBytes(entry);
            children.add(new FullBulkStringRedisMessage(buf));
        }
        response.writeArray(children);
    }
}
