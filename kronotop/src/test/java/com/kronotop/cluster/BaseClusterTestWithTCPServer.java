package com.kronotop.cluster;

import org.junit.jupiter.api.BeforeEach;

public class BaseClusterTestWithTCPServer extends BaseClusterTest {
    @BeforeEach
    public void setup() {
        addNewInstance(true);
    }
}
