---
title: "kronotop-server"
description: "Launcher script that starts a Kronotop server."
---

`kronotop-server` starts a Kronotop server. You need Java 25 or newer on the `PATH`.

## Usage

```bash
kronotop-server [options] [-D<path>=<value>...]
```

Without arguments, the server starts with the built-in defaults. Use the options to change the common settings:

```bash
kronotop-server --data-dir /var/lib/kronotop --cluster-name production
```

## How the script finds the server

The script expects the server jar at `lib/kronotop.jar`, in the directory one level above its own. For example:

```
/opt/kronotop/bin/kronotop-server
/opt/kronotop/lib/kronotop.jar
```

Keep the two files in this layout. The script finds the jar from its own location, so you can run it from any working
directory.

To call it from a directory on the `PATH`, such as `/usr/bin`, create a symlink to it.

If the jar is missing, the script prints an error and exits with code 1.

## The server process

Ctrl+C or `SIGTERM` starts a clean shutdown.

To find a running server, search the process list for `kronotop.server`:

```bash
pgrep -f kronotop.server
```

## JVM options

Set `KRONOTOP_OPTS` to pass JVM options, such as the heap size or the garbage collector:

```bash
KRONOTOP_OPTS="-Xmx8g -Xms8g -XX:+UseZGC" kronotop-server
```

Separate the options with spaces. You cannot pass an option whose value contains a space.

Do not put JVM flags such as `-Xmx` or the `-XX:` flags on the command line. The server rejects them as unknown
options.

## Configuration precedence

A setting can come from four places. When more than one sets the same value, the first in this list wins:

1. Command line options, such as `--external-port`.
2. `-D` system properties.
3. The config file given with `-c`.
4. The built-in defaults.

```bash
kronotop-server -c /etc/kronotop/kronotop.conf -Dnetwork.external.port=6000 --external-port 7000
```

The client listener binds to port 7000. Without `--external-port` it would bind to 6000, and without the `-D` property
it would use the port in the file.

Only some settings have an option. [Command Line Options](../config.md#command-line-options) lists them with the
config path each one sets. Give any other setting with `-D`. The property name is the dotted config path:

```bash
kronotop-server -Dcluster.heartbeat.interval=2
```

To see the result without starting the server, add `--print-config`. It prints the resolved Kronotop settings and
exits. `--check-config` loads the config, checks the network settings of both listeners, and exits.

`--log-level` sets the log level for the run, for example `--log-level DEBUG`.

## Starting after an unclean stop

A member keeps its [status](../admin/cluster/operations-guide.md#concepts) in the cluster metadata. On a clean
shutdown it changes the status from `RUNNING` to `STOPPED`. If the process is killed with `SIGKILL` or crashes, that
step never runs and the member stays `RUNNING`. The next start then fails:

```
Failed to start Kronotop instance: A member is already running with this id: 006cdc459c59e600c76494e8388857fc3cba2fa8
```

Kronotop stops here because it cannot tell a crashed member from one that is still running. To start anyway, set
`cluster.force_initialization` to `true`:

```bash
kronotop-server -Dcluster.force_initialization=true
```

The member then overwrites its old record and starts as usual.

Follow two rules when you use it:

- Make sure no other server process uses the same data directory.
- Pass it for that one start only. If it stays in a config file, every start skips the check.

## Exit codes

`--check-config` exits with 0 when the config is valid, and `--print-config` exits with 0 after it prints the config.
The exit code is 1 when the config is invalid, when the server fails to start, or when the script finds no jar. It is
2 when the command line has an unknown option.

## Help

```
Usage: kronotop-server [-hv] [--check-config] [--print-config] [-c=<file>]
                       [--cluster-name=<name>] [--data-dir=<path>]
                       [--external-host=<host>] [--external-port=<port>]
                       [--fdb-cluster-file=<path>] [--fdbc=<path>]
                       [--internal-host=<host>] [--internal-port=<port>]
                       [--log-level=<level>] [--netty-transport=<nio|epoll>]
                       [--external-advertise=<address>[,<address>...]]...
                       [--internal-advertise=<address>[,<address>...]]...
Starts a Kronotop server.
  -h, --help                Show help.
  -v, --version             Print version information and quit.
  -c, --config=<file>       Path to the config file.
      --data-dir=<path>     Data directory.
      --cluster-name=<name> Cluster name.
      --fdb-cluster-file=<path>
                            FoundationDB cluster file.
      --fdbc=<path>         Path to the libfdb_c library.
      --external-host=<host>
                            Bind host of the client listener.
      --external-port=<port>
                            Bind port of the client listener.
      --internal-host=<host>
                            Bind host of the internal listener.
      --internal-port=<port>
                            Bind port of the internal listener.
      --external-advertise=<address>[,<address>...]
                            Comma separated list of advertised client addresses.
      --internal-advertise=<address>[,<address>...]
                            Comma separated list of advertised internal
                              addresses.
      --netty-transport=<nio|epoll>
                            Transport for both listeners.
      --log-level=<level>   Log level.
      --check-config        Load and validate the config, then exit.
      --print-config        Print the resolved config, then exit.
```
