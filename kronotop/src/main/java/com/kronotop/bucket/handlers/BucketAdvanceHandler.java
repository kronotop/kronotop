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
import com.kronotop.bucket.BucketService;
import com.kronotop.bucket.executor.PlanExecutor;
import com.kronotop.bucket.executor.PlanExecutorConfig;
import com.kronotop.bucket.handlers.protocol.BucketAdvanceMessage;
import com.kronotop.internal.TransactionUtils;
import com.kronotop.server.*;
import com.kronotop.server.annotation.Command;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Objects;

import static com.kronotop.AsyncCommandExecutor.supplyAsync;

@Command(BucketAdvanceMessage.COMMAND)
public class BucketAdvanceHandler extends BaseBucketHandler {
    public BucketAdvanceHandler(BucketService service) {
        super(service);
    }

    @Override
    public void beforeExecute(Request request) {
        request.attr(MessageTypes.BUCKETADVANCE).set(new BucketAdvanceMessage(request));
    }

    @Override
    public void execute(Request request, Response response) throws Exception {

        supplyAsync(context, response, () -> {
            Session session = request.getSession();

            PlanExecutorConfig config = session.attr(SessionAttributes.PLAN_EXECUTOR_CONFIG).get();
            if (Objects.isNull(config)) {
                throw new KronotopException("No previous query state found in this session");
            }
            PlanExecutor executor = new PlanExecutor(context, config);
            try {
                Transaction tr = TransactionUtils.getOrCreateTransaction(service.getContext(), session);
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
