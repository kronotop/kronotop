#!/usr/bin/env sh

set -eu;

# Default JVM options (can be overridden via KRONOTOP_OPTS)
DEFAULT_JAVA_OPTS="--sun-misc-unsafe-memory-access=allow --add-opens jdk.unsupported/sun.misc=ALL-UNNAMED --enable-native-access=ALL-UNNAMED --add-modules jdk.incubator.vector"
JAVA_OPTS="${KRONOTOP_OPTS:-$DEFAULT_JAVA_OPTS}"
FDB_CLUSTER_FILE="${KR_HOME}/fdb.cluster"

# Kronotop binds to the wildcard address inside the container, so it cannot guess
# the addresses other members and clients use to reach it. Turn the comma separated
# KRONOTOP_EXTERNAL_ADVERTISE and KRONOTOP_INTERNAL_ADVERTISE lists into indexed
# system properties.
ADVERTISE_OPTS=""
add_advertise() {
    i=0
    old_ifs="$IFS"
    IFS=','
    for address in $2; do
        if [ -n "$address" ]; then
            ADVERTISE_OPTS="${ADVERTISE_OPTS} -Dnetwork.$1.advertise.${i}=${address}"
            i=$((i + 1))
        fi
    done
    IFS="$old_ifs"
}

add_advertise external "${KRONOTOP_EXTERNAL_ADVERTISE:-}"
add_advertise internal "${KRONOTOP_INTERNAL_ADVERTISE:-}"

# Only the node with KRONOTOP_INIT_FDB configures a new FoundationDB cluster.
# The other nodes wait until the cluster is ready.
if [ -n "${KRONOTOP_INIT_FDB:-}" ]; then
    echo "Checking whether the FoundationDB cluster is already configured. This check may take up to 10 seconds on first start."
    if ! /usr/bin/fdbcli -C $FDB_CLUSTER_FILE --exec status --timeout 10 ; then
        echo "No configured FoundationDB cluster found. Initializing a new one..."
        if ! fdbcli -C $FDB_CLUSTER_FILE --exec "configure new single memory ; status" --timeout 10 ; then
            echo "Unable to configure a new FoundationDB cluster."
            exit 1
        fi
    fi
    echo "FoundationDB cluster is ready."
else
    echo "Waiting for the FoundationDB cluster to be ready..."
    until /usr/bin/fdbcli -C $FDB_CLUSTER_FILE --exec status --timeout 3 > /dev/null 2>&1 ; do
        sleep 1
    done
    echo "FoundationDB cluster is ready."
fi

if [ -n "${KRONOTOP_BOOTSTRAP:-}" ]; then
    (
        echo "Waiting for Kronotop to be ready..."
        until java -jar ${KR_HOME}/kronotop-cli.jar -h $HOSTNAME -p 5484 -t 2 PING > /dev/null 2>&1; do
            sleep 1
        done
        echo "Kronotop is ready."

        if [ -n "${KRONOTOP_STANDBY_HOST:-}" ]; then
            echo "Waiting for standby at ${KRONOTOP_STANDBY_HOST}..."
            until java -jar ${KR_HOME}/kronotop-cli.jar -h "${KRONOTOP_STANDBY_HOST}" -p 5484 -t 2 PING > /dev/null 2>&1; do
                sleep 1
            done
            echo "Standby is ready."
        fi

        echo "Bootstrapping cluster..."
        java -jar ${KR_HOME}/kronotop-ctl.jar bootstrap $KRONOTOP_BOOTSTRAP
    ) &
fi

exec java $JAVA_OPTS $ADVERTISE_OPTS -Dlog.level=INFO -Dnetwork.external.host=0.0.0.0 -Dnetwork.internal.host=0.0.0.0 -Ddata_dir=/var/kronotop -jar ${KR_HOME}/kronotop.jar
