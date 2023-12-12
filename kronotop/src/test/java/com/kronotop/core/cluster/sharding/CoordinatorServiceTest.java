package com.kronotop.core.cluster.sharding;

import com.kronotop.core.Context;
import com.kronotop.core.cluster.BaseClusterTest;
import com.kronotop.core.cluster.Member;
import com.kronotop.core.cluster.MockProcessIdGeneratorImpl;
import com.kronotop.core.cluster.coordinator.CoordinatorService;
import com.kronotop.core.network.Address;
import org.junit.jupiter.api.Test;

import java.net.UnknownHostException;

public class CoordinatorServiceTest extends BaseClusterTest {

    @Test
    public void testCheckShardOwnership() throws UnknownHostException {
        Context context = newContext();
        CoordinatorService coordinatorService = new CoordinatorService(context);

        MockProcessIdGeneratorImpl processIdGenerator = new MockProcessIdGeneratorImpl();
        Member member = new Member(Address.parseString("localhost:[5484]"), processIdGenerator.getProcessID());
        coordinatorService.addMember(member);
        coordinatorService.checkShardOwnerships();
    }
}
