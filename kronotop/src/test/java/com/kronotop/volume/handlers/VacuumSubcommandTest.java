/*
 * Copyright (c) 2023-2026 Burak Sezer
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.kronotop.volume.handlers;

import com.kronotop.commands.VolumeAdminCommandBuilder;
import com.kronotop.server.Response;
import com.kronotop.server.resp3.ErrorRedisMessage;
import com.kronotop.server.resp3.SimpleStringRedisMessage;
import com.kronotop.volume.BaseNetworkedVolumeIntegrationTest;
import com.kronotop.volume.VolumeNames;
import io.lettuce.core.codec.StringCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class VacuumSubcommandTest extends BaseNetworkedVolumeIntegrationTest {
    private final String volumeName = VolumeNames.format(SHARD_KIND, SHARD_ID);

    @Test
    void shouldStartVacuum() {
        // Behavior: START command initiates vacuum on the volume and returns OK.
        VolumeAdminCommandBuilder<String, String> cmd = new VolumeAdminCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.startVacuum(volumeName, 50.0f).encode(buf);

        Object msg = runCommand(channel, buf);

        assertInstanceOf(SimpleStringRedisMessage.class, msg);
        SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
        assertEquals(Response.OK, actualMessage.content());
    }

    @Test
    void shouldDropVacuum() {
        // Behavior: DROP command removes vacuum metadata after vacuum is stopped and returns OK.
        VolumeAdminCommandBuilder<String, String> cmd = new VolumeAdminCommandBuilder<>(StringCodec.ASCII);

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.startVacuum(volumeName, 50.0f).encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, msg);
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.stopVacuum(volumeName).encode(buf);
            runCommand(channel, buf);
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.dropVacuum(volumeName).encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, msg);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
            assertEquals(Response.OK, actualMessage.content());
        }
    }

    @Test
    void shouldReturnErrorWhenStatusWithNoVacuumMetadata() {
        // Behavior: STATUS returns an error when no vacuum has ever run.
        VolumeAdminCommandBuilder<String, String> cmd = new VolumeAdminCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.statusVacuum(volumeName).encode(buf);

        Object msg = runCommand(channel, buf);

        assertInstanceOf(ErrorRedisMessage.class, msg);
        ErrorRedisMessage errorMessage = (ErrorRedisMessage) msg;
        assertTrue(errorMessage.content().contains("No active vacuum"));
    }

    @Test
    void shouldStartVacuumAfterDrop() {
        // Behavior: A new vacuum can be started after the previous one is stopped and dropped.
        VolumeAdminCommandBuilder<String, String> cmd = new VolumeAdminCommandBuilder<>(StringCodec.ASCII);

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.startVacuum(volumeName, 50.0f).encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, msg);
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.stopVacuum(volumeName).encode(buf);
            runCommand(channel, buf);
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.dropVacuum(volumeName).encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, msg);
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.startVacuum(volumeName, 50.0f).encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, msg);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
            assertEquals(Response.OK, actualMessage.content());
        }
    }

    @Test
    void shouldReturnErrorWhenDropWithNoVacuumMetadata() {
        // Behavior: DROP returns an error when no vacuum metadata exists.
        VolumeAdminCommandBuilder<String, String> cmd = new VolumeAdminCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.dropVacuum(volumeName).encode(buf);

        Object msg = runCommand(channel, buf);

        assertInstanceOf(ErrorRedisMessage.class, msg);
        ErrorRedisMessage errorMessage = (ErrorRedisMessage) msg;
        assertTrue(errorMessage.content().contains("No active vacuum"));
    }

    @Test
    void shouldReturnErrorWhenStoppingWithNoActiveVacuum() {
        // Behavior: Stopping vacuum when none is running returns an error.
        VolumeAdminCommandBuilder<String, String> cmd = new VolumeAdminCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.stopVacuum(volumeName).encode(buf);

        Object msg = runCommand(channel, buf);

        assertInstanceOf(ErrorRedisMessage.class, msg);
        ErrorRedisMessage errorMessage = (ErrorRedisMessage) msg;
        assertTrue(errorMessage.content().contains("No active vacuum"));
    }

    @Test
    void shouldReturnErrorWhenStartVacuumWithInvalidGarbageThreshold() {
        // Behavior: A garbage threshold must be between 0 and 100 (exclusive). Both 0 and 100 are rejected.
        VolumeAdminCommandBuilder<String, String> cmd = new VolumeAdminCommandBuilder<>(StringCodec.ASCII);

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.startVacuum(volumeName, 0.0f).encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(ErrorRedisMessage.class, msg);
            ErrorRedisMessage errorMessage = (ErrorRedisMessage) msg;
            assertTrue(errorMessage.content().contains("garbage-threshold must be between 0 and 100"));
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.startVacuum(volumeName, 100.0f).encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(ErrorRedisMessage.class, msg);
            ErrorRedisMessage errorMessage = (ErrorRedisMessage) msg;
            assertTrue(errorMessage.content().contains("garbage-threshold must be between 0 and 100"));
        }
    }

    @Test
    void shouldReturnErrorWhenStartVacuumWithNonNumericGarbageThreshold() {
        // Behavior: A non-numeric garbage threshold is rejected with a descriptive error.
        ByteBuf buf = Unpooled.buffer();
        buf.writeBytes("*5\r\n$12\r\nVOLUME.ADMIN\r\n$6\r\nVACUUM\r\n$5\r\nSTART\r\n$13\r\nstash-shard-1\r\n$3\r\nabc\r\n".getBytes());

        Object msg = runCommand(channel, buf);

        assertInstanceOf(ErrorRedisMessage.class, msg);
        ErrorRedisMessage errorMessage = (ErrorRedisMessage) msg;
        assertTrue(errorMessage.content().contains("garbage-threshold must be a number"));
    }

    @Test
    void shouldReturnErrorWithInvalidNumberOfParametersForStart() {
        // Behavior: START without garbage threshold parameter is rejected.
        ByteBuf buf = Unpooled.buffer();
        buf.writeBytes("*3\r\n$12\r\nVOLUME.ADMIN\r\n$6\r\nVACUUM\r\n$5\r\nSTART\r\n".getBytes());

        Object msg = runCommand(channel, buf);

        assertInstanceOf(ErrorRedisMessage.class, msg);
        ErrorRedisMessage errorMessage = (ErrorRedisMessage) msg;
        assertTrue(errorMessage.content().contains("invalid number of parameters"));
    }

    @Test
    void shouldReturnErrorWithInvalidNumberOfParametersForStop() {
        // Behavior: STOP without volume-name parameter is rejected.
        ByteBuf buf = Unpooled.buffer();
        buf.writeBytes("*3\r\n$12\r\nVOLUME.ADMIN\r\n$6\r\nVACUUM\r\n$4\r\nSTOP\r\n".getBytes());

        Object msg = runCommand(channel, buf);

        assertInstanceOf(ErrorRedisMessage.class, msg);
        ErrorRedisMessage errorMessage = (ErrorRedisMessage) msg;
        assertTrue(errorMessage.content().contains("invalid number of parameters"));
    }
}
