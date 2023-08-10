package com.kronotop.redis.connection.protocol;

import com.kronotop.BaseProtocolTest;
import com.kronotop.protocol.KronotopCommandBuilder;
import com.kronotop.server.resp.Request;
import com.kronotop.server.resp.impl.RespRequest;
import io.lettuce.core.codec.StringCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class AuthMessageTest extends BaseProtocolTest {
    @Test
    public void testAuthMessage() {
        KronotopCommandBuilder<String, String> cb = new KronotopCommandBuilder<>(StringCodec.ASCII);

        String expectedUsername = "devuser";
        String expectedPassword = "devpass";
        ByteBuf buf = Unpooled.buffer();
        cb.auth(expectedUsername, expectedPassword).encode(buf);

        channel.writeInbound(buf);
        Object msg = channel.readInbound();

        Request request = new RespRequest(null, msg);
        AuthMessage authMessage = new AuthMessage(request);

        assertEquals(expectedUsername, authMessage.getUsername());
        assertEquals(expectedPassword, authMessage.getPassword());
    }
}
