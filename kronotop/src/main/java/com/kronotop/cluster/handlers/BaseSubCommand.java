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

package com.kronotop.cluster.handlers;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.kronotop.cluster.membership.MembershipService;
import com.kronotop.common.utils.DirectoryLayout;

class BaseSubCommand {
    protected final MembershipService service;

    BaseSubCommand(MembershipService service) {
        this.service = service;
    }

    protected DirectorySubspace createOrOpenClusterMetadataSubspace() {
        DirectoryLayout clusterMetadataPath = DirectoryLayout.Builder.
                clusterName(service.getContext().getClusterName()).
                metadata();
        try (Transaction tr = service.getContext().getFoundationDB().createTransaction()) {
            DirectorySubspace subspace = DirectoryLayer.
                    getDefault().
                    createOrOpen(tr, clusterMetadataPath.asList()).
                    join();
            tr.commit().join();
            return subspace;
        }
    }
}
