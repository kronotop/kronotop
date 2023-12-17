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
