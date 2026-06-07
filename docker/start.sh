#!/usr/bin/env sh

set -eu;

# Default JVM options (can be overridden via KRONOTOP_JAVA_OPTS)
DEFAULT_JAVA_OPTS="--sun-misc-unsafe-memory-access=allow --add-opens java.base/sun.misc=ALL-UNNAMED --enable-native-access=ALL-UNNAMED --add-modules jdk.incubator.vector"
JAVA_OPTS="${KRONOTOP_JAVA_OPTS:-$DEFAULT_JAVA_OPTS}"
FDB_CLUSTER_FILE="${KR_HOME}/fdb.cluster"

if ! /usr/bin/fdbcli -C $FDB_CLUSTER_FILE --exec status --timeout 10 ; then
    echo "Initializing a new FoundationDB cluster"
    if ! fdbcli -C $FDB_CLUSTER_FILE --exec "configure new single memory ; status" --timeout 10 ; then
        echo "Unable to configure a new FoundationDB cluster."
        exit 1
    fi
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

exec java $JAVA_OPTS -Dnetwork.external.host=0.0.0.0 -Dnetwork.internal.host=0.0.0.0 -Ddata_dir=/var/kronotop -jar ${KR_HOME}/kronotop.jar
