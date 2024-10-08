/*
 * Copyright (c) 2023-2024 Kronotop
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

package com.kronotop.volume;

import com.kronotop.BaseTest;
import com.kronotop.volume.replication.Host;
import org.junit.jupiter.api.Test;

import java.net.UnknownHostException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class VolumeMetadataTest extends BaseTest {
    @Test
    public void test_addSegment() {
        VolumeMetadata volumeMetadata = new VolumeMetadata();
        volumeMetadata.addSegment(1);

        assertThrows(IllegalArgumentException.class, () -> volumeMetadata.addSegment(1));
    }

    @Test
    public void test_removeSegment() {
        VolumeMetadata volumeMetadata = new VolumeMetadata();
        volumeMetadata.addSegment(1);
        volumeMetadata.removeSegment(1);

        assertThrows(IllegalArgumentException.class, () -> volumeMetadata.removeSegment(1));
    }

    @Test
    public void test_setOwner() throws UnknownHostException {
        VolumeMetadata volumeMetadata = new VolumeMetadata();
        Host owner = new Host(Role.OWNER, createMemberWithEphemeralPort());
        volumeMetadata.setOwner(owner);
        assertEquals(owner, volumeMetadata.getOwner());
    }

    @Test
    public void test_setOwner_idempotency() throws UnknownHostException {
        VolumeMetadata volumeMetadata = new VolumeMetadata();
        Host owner = new Host(Role.OWNER, createMemberWithEphemeralPort());
        volumeMetadata.setOwner(owner);
        volumeMetadata.setOwner(owner);
        assertEquals(1, volumeMetadata.getHosts().size());
    }

    @Test
    public void test_when_no_owner_found() {
        VolumeMetadata volumeMetadata = new VolumeMetadata();
        assertThrows(IllegalStateException.class, volumeMetadata::getOwner);
    }

    @Test
    public void test_setOwner_IllegalArgumentException() throws UnknownHostException {
        VolumeMetadata volumeMetadata = new VolumeMetadata();
        Host owner = new Host(Role.STANDBY, createMemberWithEphemeralPort());
        assertThrows(IllegalArgumentException.class, () -> volumeMetadata.setOwner(owner));
    }

    @Test
    public void test_setStandby() throws UnknownHostException {
        VolumeMetadata volumeMetadata = new VolumeMetadata();
        Host standby = new Host(Role.STANDBY, createMemberWithEphemeralPort());
        volumeMetadata.setStandby(standby);
        assertEquals(standby, volumeMetadata.getStandbyHosts().getFirst());
    }

    @Test
    public void test_setStandby_idempotency() throws UnknownHostException {
        VolumeMetadata volumeMetadata = new VolumeMetadata();
        Host standby = new Host(Role.STANDBY, createMemberWithEphemeralPort());
        volumeMetadata.setStandby(standby);
        volumeMetadata.setStandby(standby);
        assertEquals(1, volumeMetadata.getStandbyHosts().size());
    }

    @Test
    public void test_unsetOwner() throws UnknownHostException {
        VolumeMetadata volumeMetadata = new VolumeMetadata();
        Host owner = new Host(Role.OWNER, createMemberWithEphemeralPort());
        volumeMetadata.setOwner(owner);
        volumeMetadata.unsetOwner(owner);
        assertThrows(IllegalStateException.class, volumeMetadata::getOwner);
    }

    @Test
    public void test_unsetStandby() throws UnknownHostException {
        VolumeMetadata volumeMetadata = new VolumeMetadata();

        Host owner = new Host(Role.OWNER, createMemberWithEphemeralPort());
        volumeMetadata.setOwner(owner);

        Host standby = new Host(Role.STANDBY, createMemberWithEphemeralPort());
        volumeMetadata.setStandby(standby);

        volumeMetadata.unsetStandby(standby);

        assertEquals(0, volumeMetadata.getStandbyHosts().size());
    }
}