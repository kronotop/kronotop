package com.kronotop.redis.connection;

import com.kronotop.redis.BaseHandlerTest;
import com.kronotop.redistest.RedisCommandBuilder;
import com.kronotop.server.resp3.SimpleStringRedisMessage;
import com.typesafe.config.Config;
import io.lettuce.core.codec.StringCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.UnknownHostException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

public class AuthHandlerRequirePassTest extends BaseHandlerTest {
    @Override
    @BeforeEach
    public void setup() throws UnknownHostException, InterruptedException {
        Config config = loadConfig("auth-requirepass-test.conf");
        setupCommon(config);
    }

    @Test
    public void testAuthOnlyWithPass() {
        RedisCommandBuilder<String, String> cmd = new RedisCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.auth("devpass").encode(buf);

        channel.writeInbound(buf);
        Object msg = channel.readOutbound();
        assertInstanceOf(SimpleStringRedisMessage.class, msg);
        SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
        assertEquals("OK", actualMessage.content());
    }
}
