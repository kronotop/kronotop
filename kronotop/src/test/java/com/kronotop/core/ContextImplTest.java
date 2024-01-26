package com.kronotop.core;

import com.apple.foundationdb.Database;
import com.kronotop.ConfigTestUtil;
import com.kronotop.core.cluster.Member;
import com.kronotop.core.cluster.MockProcessIdGeneratorImpl;
import com.kronotop.core.network.Address;
import com.kronotop.foundationdb.FoundationDBService;
import com.kronotop.server.Handlers;
import com.typesafe.config.Config;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.UnknownHostException;

class ContextImplTest {
    private FoundationDBService service;
    private Handlers handlers;
    private Context context;

    @BeforeEach
    public void setup() throws UnknownHostException {
        MockProcessIdGeneratorImpl processIdGenerator = new MockProcessIdGeneratorImpl();
        Config config = ConfigTestUtil.load("test.conf");
        Address address = new Address("localhost", 0);
        Member member = new Member(address, processIdGenerator.getProcessID());
        Database database = FoundationDBFactory.newDatabase(config);
        context = new ContextImpl(config, member, database);
        handlers = new Handlers();
        service = new FoundationDBService(context, handlers);
    }

    @AfterEach
    public void teardown() {
        service.shutdown();
    }

    @Test
    public void testDatabaseConnection() {
        //System.out.println(context.openDirectorySubspace(List.of("foobar")));
    }
}