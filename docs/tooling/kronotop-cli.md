---
title: "kronotop-cli"
description: "Interactive command-line client for Kronotop."
---

`kronotop-cli` is the command-line client for Kronotop. It runs in three modes:

- **Interactive mode**: started when no command is given. Provides a prompt with colorized output.
- **One-shot mode**: pass a command as arguments, get the reply, and exit. Use `-r` and `-i` to repeat it at an
  interval.
- **Script mode**: execute commands from a file with `-f`.

Sessions use RESP3 by default; switch to RESP2 with `-2`. Replies can be formatted as JSON, CSV, or raw output.
TLS connections are supported.

`kronotop-cli` also sets the session attributes `input_type`, `reply_type`, and `object_id_format` automatically,
so document replies are human-readable by default.

## Usage

Run the jar directly:

```bash
java -jar kronotop-cli-2026.06-3.jar -h localhost -p 5484
```

Or use the Docker image:

```bash
docker run --rm -it --network kronotop \
  ghcr.io/kronotop/kronotop:latest kronotop-cli -h kronotop-primary -p 5484
```

## One-Shot Mode

Pass a command after the options to execute it once and exit:

```bash
kronotop-cli -p 5484 BUCKET.QUERY orders '{"qty": {"$gte": 1}}'
```

Each shell argument becomes one command argument, so wrap JSON documents or anything containing spaces in
quotes. The escape sequences `\n`, `\r`, `\t`, and `\xHH` inside arguments are processed before the command
is sent.

`-r` executes the same command N times. `-i` adds a wait between runs, and sub-second values are allowed.
Responses are separated by the `-D` delimiter, a newline by default. Useful for watching cluster state:

```bash
kronotop-cli -p 3320 -r 10 -i 0.5 KR.ADMIN DESCRIBE-CLUSTER
```

When stdout is not a terminal, replies switch to raw formatting, which is easier to parse in shell pipelines. Use
`--no-raw` to keep formatted output, or `--json` and `--csv` to pick a structured format:

```bash
kronotop-cli -p 5484 --json BUCKET.QUERY orders '{"qty": {"$gte": 1}}'
```

## Script Mode

`-f` executes commands from a file, one command per line. Blank lines and lines starting with `#` are skipped.
Execution stops at the first error: the failing line number is printed to stderr and the exit code is non-zero.
If the error happens inside an open transaction, the transaction is rolled back automatically before the script
aborts.

Two directives are handled by the client itself:

- `.echo <text>`: prints the text. The `$TIMESTAMP` variable expands to the current time.
- `.sleep <duration>`: pauses the script. Accepts `ms`, `s`, `m`, and `h` units, for example `100ms` or `2s`.

A script that seeds a bucket inside a single transaction:

```
# seed.txt: create the orders bucket and load a few documents
.echo Seeding orders at $TIMESTAMP

BUCKET.CREATE orders

BEGIN
BUCKET.INSERT orders DOCS '{"item": "keyboard", "qty": 2, "price": 49.99}'
BUCKET.INSERT orders DOCS '{"item": "mouse", "qty": 5, "price": 19.99}'
COMMIT

.echo Done
```

Run it:

```bash
kronotop-cli -p 5484 -f seed.txt
```

Each reply is printed as the script runs. Pass `-s` to suppress replies; errors are still printed to stderr.

## Help

```
Usage: kronotop-cli [-23sv] [--csv] [--help] [--insecure] [--json] [--no-color]
                    [--no-raw] [--quoted-input] [--quoted-json] [--raw] [--tls]
                    [--cacert=<cacert>] [--cacertdir=<cacertdir>]
                    [-d=<delimiter>] [-D=<responseDelimiter>] [-f=<scriptFile>]
                    [-h=<host>] [-i=<interval>] [--input-type=<inputType>]
                    [--object-id-format=<objectIdFormat>] [-p=<port>]
                    [-r=<repeat>] [--reply-type=<replyType>] [--sni=<sni>]
                    [-t=<timeout>] [--tls-ciphers=<tlsCiphers>]
                    [--tls-ciphersuites=<tlsCiphersuites>] [<command>...]
Command-line interface for Kronotop.
When no command is given, kronotop-cli starts in interactive mode.
      [<command>...]        Command to execute.
  -h, --host=<host>         Server hostname.
  -p, --port=<port>         Server port.
  -t, --timeout=<timeout>   Server connection timeout in seconds (decimals
                              allowed). Default is 0, meaning no limit.
  -r=<repeat>               Execute specified command N times.
  -i=<interval>             When -r is used, waits <interval> seconds per
                              command. Sub-second times allowed (e.g., -i 0.1).
  -2, --resp-2              Start session in RESP2 protocol mode (default:
                              RESP3).
  -3, --resp-3              Start session in RESP3 protocol mode (default).
      --json                Output in JSON format (default RESP3, use -2 if you
                              want to use with RESP2).
      --quoted-json         Same as --json, but produce ASCII-safe quoted
                              strings, not Unicode.
      --csv                 Output in CSV format.
      --raw                 Use raw formatting for replies (default when STDOUT
                              is not a tty).
      --no-raw              Force formatted output even when STDOUT is not a
                              tty.
  -d=<delimiter>            Delimiter between response bulks for raw formatting.
  -D=<responseDelimiter>    Delimiter between responses for raw formatting.
      --quoted-input        Force input to be handled as quoted strings.
  -v, --version             Print version information and quit.
      --help                Show help.
  -f, --file=<scriptFile>   Execute commands from a script file.
      --no-color            Disable colorized output.
  -s, --silent              Suppress command output in script mode (-f). Errors
                              are still printed to stderr.
      --input-type=<inputType>
                            Session input type. Valid values: JSON, BSON.
                              Default: JSON.
      --reply-type=<replyType>
                            Session reply type. Valid values: JSON, BSON.
                              Default: JSON.
      --object-id-format=<objectIdFormat>
                            ObjectId format. Valid values: hex, bytes. Default:
                              hex.
      --tls                 Establish a secure TLS connection.
      --sni=<sni>           Server name indication for TLS.
      --cacert=<cacert>     CA certificate file to verify with (PEM format).
      --cacertdir=<cacertdir>
                            Directory of trusted CA certificate files (PEM
                              format).
      --insecure            Allow insecure TLS connection by skipping cert
                              validation.
      --tls-ciphers=<tlsCiphers>
                            Colon-separated list of TLSv1.2 cipher suites.
      --tls-ciphersuites=<tlsCiphersuites>
                            Colon-separated list of TLSv1.3 cipher suites.
```
