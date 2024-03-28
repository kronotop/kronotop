package com.kronotop.client;

import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import org.junit.jupiter.api.Test;

public class KronotopClientTest {
    @Test
    public void test_foo() {
        try (RedisClusterClient redisClient = RedisClusterClient.create("redis://localhost:5484")) {
            StatefulRedisClusterConnection<String, String> connection = redisClient.connect();
            StatefulKronotopConnection<String, String> krConn = KronotopClient.connect(redisClient);
            System.out.println(krConn.sync().zget("buraksezer"));
            //krConn.sync().sql(SqlArgs.Builder.query(""));
        }
    }
}
