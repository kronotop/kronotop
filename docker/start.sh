#!/usr/bin/env sh

set -eu;

FDB_CLUSTER_FILE="${KR_HOME}/fdb.cluster"

if ! /usr/bin/fdbcli -C $FDB_CLUSTER_FILE --exec status --timeout 3 ; then
    echo "Initializing a new FoundationDB cluster"
    if ! fdbcli -C $FDB_CLUSTER_FILE --exec "configure new single memory ; status" --timeout 10 ; then
        echo "Unable to configure a new FoundationDB cluster."
        exit 1
    fi
fi

exec java -Dnetwork.external.host=0.0.0.0 -Dnetwork.internal.host=0.0.0.0 -Ddata_dir=/var/kronotop -jar ${KR_HOME}/kronotop-SNAPSHOT.jar