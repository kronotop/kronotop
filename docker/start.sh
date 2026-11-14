#!/usr/bin/env bash

set -eu;

FDB_CLUSTER_FILE="${KR_HOME}/fdb.cluster"

# Kronotop binds to the wildcard address inside the container, so it cannot guess
# the addresses other members and clients use to reach it. Pass the comma separated
# KRONOTOP_EXTERNAL_ADVERTISE and KRONOTOP_INTERNAL_ADVERTISE lists when they are set.
ADVERTISE_OPTS=""
if [ -n "${KRONOTOP_EXTERNAL_ADVERTISE:-}" ]; then
    ADVERTISE_OPTS="${ADVERTISE_OPTS} --external-advertise ${KRONOTOP_EXTERNAL_ADVERTISE}"
fi
if [ -n "${KRONOTOP_INTERNAL_ADVERTISE:-}" ]; then
    ADVERTISE_OPTS="${ADVERTISE_OPTS} --internal-advertise ${KRONOTOP_INTERNAL_ADVERTISE}"
fi

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

exec kronotop-server --log-level INFO --external-host 0.0.0.0 --internal-host 0.0.0.0 --data-dir /var/kronotop $ADVERTISE_OPTS
