// Copyright (C) 2025 Burak Sezer
// Use of this software is governed by the Business Source License included
// in the LICENSE.TXT file and at www.mariadb.com/bsl11.

// Change Date: 5 years after release

// On the date above, in accordance with the Business Source License,
// use of this software will be governed by the open source license specified
// in the LICENSE.TXT file.

package com.kronotop.bucket.handlers;

import com.apple.foundationdb.Transaction;
import com.kronotop.KronotopException;
import com.kronotop.bucket.*;
import com.kronotop.bucket.executor.PlanExecutor;
import com.kronotop.bucket.executor.PlanExecutorConfig;
import com.kronotop.bucket.executor.PlanExecutorEnvironment;
import com.kronotop.bucket.handlers.protocol.BucketQueryMessage;
import com.kronotop.bucket.index.Index;
import com.kronotop.bucket.planner.physical.PhysicalNode;
import com.kronotop.internal.TransactionUtils;
import com.kronotop.server.*;
import com.kronotop.server.annotation.Command;
import com.kronotop.server.annotation.MinimumParameterCount;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;

import static com.kronotop.AsyncCommandExecutor.supplyAsync;

@Command(BucketQueryMessage.COMMAND)
@MinimumParameterCount(BucketQueryMessage.MINIMUM_PARAMETER_COUNT)
public class BucketQueryHandler extends BaseBucketHandler implements Handler {

    public BucketQueryHandler(BucketService service) {
        super(service);
    }

    @Override
    public void beforeExecute(Request request) {
        request.attr(MessageTypes.BUCKETQUERY).set(new BucketQueryMessage(request));
    }

    public PlanExecutorConfig preparePlanExecutorConfig(
            Transaction tr,
            Session session,
            BucketQueryMessage message,
            BucketSubspace subspace,
            BucketShard shard,
            PhysicalNode plan
    ) {
        PlanExecutorEnvironment environment = new PlanExecutorEnvironment(message.getBucket(), subspace, shard, plan);
        PlanExecutorConfig config = new PlanExecutorConfig(environment);

        if (message.getArguments().limit() == 0) {
            config.setLimit(
                    session.attr(SessionAttributes.LIMIT).get()
            );
        } else {
            config.setLimit(message.getArguments().limit());
        }
        config.setReverse(message.getArguments().reverse());

        boolean pinReadVersion = session.attr(SessionAttributes.PIN_READ_VERSION).get();
        config.setPinReadVersion(pinReadVersion);

        tr.getReadVersion().thenAccept(config::setReadVersion);

        // Now we have a validated and sanitized plan executor config.
        session.attr(SessionAttributes.PLAN_EXECUTOR_CONFIG).set(config);
        return config;
    }

    @Override
    public void execute(Request request, Response response) throws Exception {
        supplyAsync(context, response, () -> {
            BucketQueryMessage message = request.attr(MessageTypes.BUCKETQUERY).get();

            BucketShard shard = getOrSelectBucketShardId(message.getArguments().shard());

            Session session = request.getSession();

            Transaction tr = TransactionUtils.getOrCreateTransaction(service.getContext(), session);
            BucketSubspace subspace = BucketSubspaceUtils.open(context, session, tr);

            // ID is the default index
            Map<String, Index> indexes = Map.of(
                    DefaultIndex.ID.path(), DefaultIndex.ID
            );

            PhysicalNode plan = service.getPlanner().plan(indexes, message.getQuery());
            PlanExecutorConfig config = preparePlanExecutorConfig(tr, session, message, subspace, shard, plan);
            PlanExecutor executor = new PlanExecutor(context, config);
            try {
                return executor.execute(tr);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }, (entries) -> {
            RESPVersion protoVer = request.getSession().protocolVersion();
            if (protoVer.equals(RESPVersion.RESP3)) {
                resp3Response(request, response, entries);
            } else if (protoVer.equals(RESPVersion.RESP2)) {
                resp2Response(request, response, entries);
            } else {
                throw new KronotopException("Unknown protocol version " + protoVer.getValue());
            }
        });
    }
}
