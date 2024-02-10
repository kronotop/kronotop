package com.kronotop.redis.cluster;

import com.kronotop.core.cluster.Member;
import com.kronotop.redis.BaseHandler;
import com.kronotop.redis.RedisService;
import com.kronotop.redis.cluster.protocol.ClusterMessage;
import com.kronotop.server.Handler;
import com.kronotop.server.MessageTypes;
import com.kronotop.server.Request;
import com.kronotop.server.Response;
import com.kronotop.server.annotation.Command;
import com.kronotop.server.annotation.MinimumParameterCount;
import com.kronotop.server.resp3.ArrayRedisMessage;
import com.kronotop.server.resp3.FullBulkStringRedisMessage;
import com.kronotop.server.resp3.IntegerRedisMessage;
import com.kronotop.server.resp3.RedisMessage;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;

import java.util.ArrayList;
import java.util.List;

@Command(ClusterMessage.COMMAND)
@MinimumParameterCount(ClusterMessage.MINIMUM_PARAMETER_COUNT)
public class ClusterHandler extends BaseHandler implements Handler {
    public ClusterHandler(RedisService service) {
        super(service);
    }

    @Override
    public void beforeExecute(Request request) {
        request.attr(MessageTypes.CLUSTER).set(new ClusterMessage(request));
    }

    private List<RedisMessage> prepareMember(ChannelHandlerContext context, Member member) {
        List<RedisMessage> result = new ArrayList<>();
        // HOST
        ByteBuf hostBuf = context.alloc().buffer();
        hostBuf.writeBytes(member.getAddress().getHost().getBytes());
        FullBulkStringRedisMessage fullBulkStringRedisMessage = new FullBulkStringRedisMessage(hostBuf);
        result.add(fullBulkStringRedisMessage);

        // PORT
        IntegerRedisMessage integerRedisMessage = new IntegerRedisMessage(member.getAddress().getPort());
        result.add(integerRedisMessage);

        // ID
        ByteBuf idBuf = context.alloc().buffer();
        idBuf.writeBytes(member.getId().getBytes());
        FullBulkStringRedisMessage idMessage = new FullBulkStringRedisMessage(idBuf);
        result.add(idMessage);

        return result;
    }

    @Override
    public void execute(Request request, Response response) throws Exception {
        List<Range> ranges = new ArrayList<>();
        Range currentRange = new Range(0);
        Integer currentShardId = null;
        int lastHashSlot = 0;
        for (int hashSlot = 0; hashSlot < service.NUM_HASH_SLOTS; hashSlot++) {
            int shardId = service.getHashSlots().get(hashSlot);
            if (currentShardId != null && shardId != currentShardId) {
                currentRange.setEnd(hashSlot - 1);
                Member owner = service.getClusterService().getRoutingTable().getRoute(currentShardId).getMember();
                currentRange.setOwner(owner);
                ranges.add(currentRange);
                currentRange = new Range(hashSlot);
            }
            currentShardId = shardId;
            lastHashSlot = hashSlot;
        }

        currentRange.setEnd(lastHashSlot + 1);
        Member owner = service.getClusterService().getRoutingTable().getRoute(currentShardId).getMember();
        currentRange.setOwner(owner);
        ranges.add(currentRange);

        List<RedisMessage> root = new ArrayList<>();
        for (Range r : ranges) {
            List<RedisMessage> children = new ArrayList<>();
            IntegerRedisMessage beginSection = new IntegerRedisMessage(r.begin);
            IntegerRedisMessage endSection = new IntegerRedisMessage(r.end);
            ArrayRedisMessage ownerSection = new ArrayRedisMessage(prepareMember(request.getChannelContext(), r.owner));
            children.add(beginSection);
            children.add(endSection);
            children.add(ownerSection);
            root.add(new ArrayRedisMessage(children));
        }

        response.writeArray(root);
    }

    private static class Range {
        int begin;
        int end;
        Member owner;

        public Range(int begin) {
            this.begin = begin;
        }

        public void setOwner(Member owner) {
            this.owner = owner;
        }

        public void setEnd(int end) {
            this.end = end;
        }

        @Override
        public String toString() {
            return String.format("Range {begin=%d end=%d owner=%s}", begin, end, owner);
        }
    }
}
