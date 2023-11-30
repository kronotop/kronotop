/*
 * Copyright (c) 2023 Kronotop
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

package com.kronotop.core.cluster;

import com.apple.foundationdb.directory.DirectoryLayer;
import com.kronotop.common.utils.DirectoryLayout;
import com.kronotop.core.Context;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class MembershipServiceTest extends BaseClusterTest {

    @AfterEach
    public void teardown() {
        database.run(tr -> {
            DirectoryLayer directoryLayer = DirectoryLayer.getDefault();
            List<String> subpath = DirectoryLayout.Builder.clusterName(config.getString("cluster.name")).asList();
            return directoryLayer.remove(tr, subpath).join();
        });
        super.teardown();
    }

    @Test
    public void testClusterService() {
        int numMembers = 3;
        List<MembershipService> services = new ArrayList<>();
        for (int i = 0; i < numMembers; i++) {
            Context context = newContext();
            MembershipService membershipService = new MembershipService(context);
            membershipService.start();
            services.add(membershipService);
        }

        MembershipService membershipService = services.get(0);
        assertEquals(numMembers, membershipService.getMembers().size());

        for (MembershipService service : services) {
            service.shutdown();
        }
    }
}
