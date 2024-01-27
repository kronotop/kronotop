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

package com.kronotop.core;

import com.kronotop.common.utils.DirectoryLayout;

/**
 * The ClusterLayout class provides methods for constructing directory paths specific to a cluster.
 */
public class ClusterLayout {
    public static DirectoryLayout getRoot(Context context) {
        return DirectoryLayout.Builder.clusterName(context.getClusterName());
    }

    public static DirectoryLayout getInternal(Context context) {
        return getRoot(context).internal();
    }

    public static DirectoryLayout getShards(Context context) {
        return getInternal(context).shards();
    }

    public static DirectoryLayout getCluster(Context context) {
        return getInternal(context).cluster();
    }

    public static DirectoryLayout getMemberlist(Context context) {
        return getCluster(context).memberlist();
    }
}
