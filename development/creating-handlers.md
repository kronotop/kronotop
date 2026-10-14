# Creating Command Handlers

**Disclose**: This document is created and edited by various Claude models. It's sole purpose is to document the complex handler
creation process.

Kronotop processes RESP commands with handlers. Each command has a handler class that implements the `Handler`
interface. A service registers its handlers, and the server calls the matching handler when a command arrives.

`ZDelHandler` (`com.kronotop.zmap.handlers.ZDelHandler`) is the reference handler. Read it together with this guide.

## Steps

The examples add a `MYCOMMAND <key> [NAMESPACE <path>]` command to ZMap. It deletes a key.

### Step 1: Add the Command to the Server CommandType Enum

Add the command to `com.kronotop.server.CommandType`:

```java
public enum CommandType {
    // ... existing commands ...

    MYCOMMAND,

    // A command with a dot in its name takes the wire name as a parameter
    MY_COMMAND("MY.COMMAND"),

    // ... rest of enum ...
}
```

Add it to the `parse()` switch too:

```java
public static CommandType parse(String command) {
    return switch (command) {
        // ... existing cases ...
        case "MYCOMMAND" -> MYCOMMAND;
        case "MY.COMMAND" -> MY_COMMAND;
        // ...
        default -> null;
    };
}
```

`CommandHandlerService` throws `IllegalArgumentException` at startup if a handler's command is not in this enum.

### Step 2: Create the Protocol Message Class

The message class parses the command arguments. Put it in the `protocol` subpackage of your feature's handlers.

- `request.getArguments()` returns the arguments as `ByteBuf`s. The command name is not in the list.
- Use `ProtocolMessageUtil` (`com.kronotop.internal.ProtocolMessageUtil`) to read them: `readAsByteArray`,
  `readAsString`, `readAsLong`, `readAsInteger`, `readAsDouble`, `readEnum`.
- Read the optional `NAMESPACE <path>` argument with `readTrailingNamespace(arguments, startIndex)`. It returns `null`
  when the argument is not given and rejects unknown keywords.
- For other optional keywords, use `requireValue(arguments, i, keyword, expected)` to read the value after a keyword.
  It throws the standard error when the value is missing. Use `markArgumentSeen(seen, key)` to reject a keyword that
  is given twice.
- For a bad argument, throw `IllegalCommandArgumentException`. `ProtocolMessageUtil.illegalValue` builds a standard one.

**Example: `MyCommandMessage.java`**

```java
package com.kronotop.zmap.handlers.protocol;

import com.kronotop.internal.ProtocolMessageUtil;
import com.kronotop.server.ProtocolMessage;
import com.kronotop.server.Request;

import java.util.List;

public class MyCommandMessage implements ProtocolMessage<byte[]> {
    public static final String COMMAND = "MYCOMMAND";
    public static final int MINIMUM_ARGUMENT_COUNT = 1;
    public static final int MAXIMUM_ARGUMENT_COUNT = 3;

    private final Request request;
    private byte[] key;
    private String namespace;

    public MyCommandMessage(Request request) {
        this.request = request;
        parse();
    }

    private void parse() {
        key = ProtocolMessageUtil.readAsByteArray(request.getArguments().get(0));
        namespace = ProtocolMessageUtil.readTrailingNamespace(request.getArguments(), 1);
    }

    /**
     * Returns the namespace given on the command, or null if not specified.
     */
    public String getNamespace() {
        return namespace;
    }

    @Override
    public byte[] getKey() {
        return key;
    }

    @Override
    public List<byte[]> getKeys() {
        return null;
    }
}
```

### Step 3: Add an AttributeKey to MessageTypes

Register the message type in `com.kronotop.server.MessageTypes`:

```java
public static final AttributeKey<MyCommandMessage> MYCOMMAND = AttributeKey.valueOf(MyCommandMessage.COMMAND);
```

### Step 4: Create the Handler Class

Rules for a handler:

- The handler gets its service in the constructor and takes `context` from it. For ZMap, `BaseZMapHandler` does
  this. It also returns `false` from `isRedisCompatible()`, which is right for Kronotop commands.
- Never do FDB work on the Netty thread. Use `AsyncCommandExecutor.runAsync(context, response, work, reply)` when
  there is no result, and `supplyAsync(context, response, supplier, consumer)` when there is one (see `ZGetHandler`).
  The work runs on a virtual thread. The reply runs on the Netty thread. Both methods also clean up auto-commit
  transactions.
- Get the transaction with `TransactionUtil.getOrCreateTransaction(context, session)`. Finish with
  `TransactionUtil.commitIfAutoCommitEnabled(tr, session)`. Do not close the transaction in the handler; `Session`
  closes it. Never create a transaction in any other way.
- Open the data structure subspace with the namespace from the message. `null` means the session's current
  namespace. ZMap handlers call `openZMapSubspace(tr, session, namespace)`, which uses `NamespaceUtil.resolve` and
  `NamespaceUtil.openDataStructureSubspace`.
- Throw `KronotopException` or `IllegalCommandArgumentException`. Do not call `writeError` yourself. The server and
  `AsyncCommandExecutor` turn exceptions and `FDBException`s into RESP errors.
- If the command writes keys, return `true` from `isWatchable()` and return the written keys from `getKeys(Request)`.
- ZMap handlers count their operations on `service.getMetrics()` at the start of `execute()`. Look at
  `ZMapMetrics` for the counter that fits the command.

**Example: `MyCommandHandler.java`**

```java
package com.kronotop.zmap.handlers;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.kronotop.AsyncCommandExecutor;
import com.kronotop.server.*;
import com.kronotop.server.annotation.Command;
import com.kronotop.server.annotation.MaximumArgumentCount;
import com.kronotop.server.annotation.MinimumArgumentCount;
import com.kronotop.transaction.TransactionUtil;
import com.kronotop.zmap.BaseZMapHandler;
import com.kronotop.zmap.ZMapService;
import com.kronotop.zmap.handlers.protocol.MyCommandMessage;

import java.util.Collections;
import java.util.List;

@Command(MyCommandMessage.COMMAND)
@MinimumArgumentCount(MyCommandMessage.MINIMUM_ARGUMENT_COUNT)
@MaximumArgumentCount(MyCommandMessage.MAXIMUM_ARGUMENT_COUNT)
public class MyCommandHandler extends BaseZMapHandler implements Handler {
    public MyCommandHandler(ZMapService service) {
        super(service);
    }

    @Override
    public boolean isWatchable() {
        return true;
    }

    @Override
    public void beforeExecute(Request request) {
        request.attr(MessageTypes.MYCOMMAND).set(new MyCommandMessage(request));
    }

    @Override
    public List<String> getKeys(Request request) {
        return Collections.singletonList(new String(request.attr(MessageTypes.MYCOMMAND).get().getKey()));
    }

    @Override
    public void execute(Request request, Response response) {
        service.getMetrics().incrementDeletes();
        AsyncCommandExecutor.runAsync(context, response, () -> {
            MyCommandMessage message = request.attr(MessageTypes.MYCOMMAND).get();

            Session session = request.getSession();
            Transaction tr = TransactionUtil.getOrCreateTransaction(context, session);
            DirectorySubspace subspace = openZMapSubspace(tr, session, message.getNamespace());

            tr.clear(subspace.pack(message.getKey()));
            TransactionUtil.commitIfAutoCommitEnabled(tr, session);
        }, response::writeOK);
    }
}
```

### Step 5: Register the Handler in Its Service

Register the handler in its service, for example `ZMapService`:

```java
public ZMapService(Context context) {
    super(context, NAME);

    // ... existing handlers ...

    // For external client connections
    handlerMethod(ServerKind.EXTERNAL, new MyCommandHandler(this));

    // Only if cluster members must call it too
    handlerMethod(ServerKind.INTERNAL, new MyCommandHandler(this));
}
```

### Step 6: Add the Command Metadata JSON

Add `kronotop/src/main/resources/commands/mycommand.json`. Copy `zdel.json` and edit it. `COMMAND`, `COMMAND INFO`
and `COMMAND DOCS` return this data.

- `servers` must list the same `ServerKind` values that you use in `handlerMethod`.
- `arity` counts the command name too. A positive value is an exact count. A negative value is a minimum.
  `MYCOMMAND <key> [NAMESPACE <path>]` has an arity of `-2`. `COMMAND GETKEYS` checks this value, so it must match
  the argument count annotations.
- `since` is the version that adds the command. Do not copy the value from `zdel.json`.
- `group`, `command_flags` and `acl_categories` take their values from the `CommandGroup`, `CommandFlag` and
  `AclCategory` enums in `com.kronotop.commands`. If a new feature needs a new group or category, add it to the enum
  first.
- `CommandMetadataLoader` fails at startup with `IllegalStateException` if the command is not in the server
  `CommandType` enum, if `servers` is empty, or if the file has an unknown field or value.
- `CommandHandlerTest.shouldMatchRegisteredCommandsPerServer` checks that metadata and registered handlers match.

### Step 7: Add the Command to the Client Side

The `kronotop-commands` module has a second `CommandType` enum: `com.kronotop.commands.CommandType`. Add the command
there with the same name. A command with a dot in its name takes the wire name as a parameter here too, for example
`MY_COMMAND("MY.COMMAND")`. Then add a method to the command builder of your feature, for example
`ZMapCommandBuilder.zdel`:

```java
public Command<K, V, String> mycommand(K key, String namespace) {
    CommandArgs<K, V> args = new CommandArgs<>(codec).addKey(key);
    if (namespace != null) {
        args.add("NAMESPACE").add(namespace);
    }
    return createCommand(CommandType.MYCOMMAND, new StatusOutput<>(codec), args);
}
```

Standard Redis commands go to `RedisCommandBuilder`. Kronotop commands go to the builder of their feature.

### Step 8: Write the User Doc

Add `docs/<area>/commands/mycommand.md` and a row for it in the command table of `docs/<area>/index.md`. For ZMap,
see `docs/zmap/commands/zdel.md` and `docs/zmap/index.md`.

### Step 9: Write the Tests

- Extend `BaseHandlerTest`. It gives you `channel`, a random `namespace`, `runCommand(channel, buf)`,
  `assertOK(response)` and `createNamespace(channel, name)`.
- Encode the command with the builder from Step 7, run it, and check the response type and content.
- Cover argument errors with a `@ParameterizedTest`: too few, too many, unknown keyword, keyword without value. The
  builder cannot produce a broken payload, so send raw arguments with `runRaw(channel, CommandType.MYCOMMAND,
  rawArgs)` from `BaseTest`. `CommandType` here is the client enum from Step 7, not the server enum.
- Cover the `NAMESPACE` argument: the command works in the given namespace and leaves the session's namespace
  unchanged; an unknown namespace returns `NOSUCHNAMESPACE`. See `shouldDeleteKeyInGivenNamespace` and
  `shouldRejectDeleteWhenNamespaceDoesNotExist` in `ZDelHandlerTest`.
- Shard kind in new tests is always `BUCKET`.
- Do not use Mockito.

See `ZDelHandlerTest` for a full example.

```java
class MyCommandHandlerTest extends BaseHandlerTest {

    static Stream<Arguments> invalidArguments() {
        return Stream.of(
                arguments("too few arguments",
                        List.of(),
                        "ERR wrong number of arguments for 'MYCOMMAND' command"),
                arguments("too many arguments",
                        List.of("key", "NAMESPACE", "ns", "EXTRA"),
                        "ERR wrong number of arguments for 'MYCOMMAND' command"),
                arguments("unknown keyword",
                        List.of("key", "EXTRA"),
                        "ERR Unknown 'EXTRA' argument"),
                arguments("NAMESPACE without value",
                        List.of("key", "NAMESPACE"),
                        "ERR NAMESPACE argument must be followed by a namespace")
        );
    }

    @Test
    void shouldDeleteKey() {
        ZMapCommandBuilder<String, String> cmd = new ZMapCommandBuilder<>(StringCodec.ASCII);

        ByteBuf buf = Unpooled.buffer();
        cmd.mycommand("key", null).encode(buf);

        Object response = runCommand(channel, buf);
        assertInstanceOf(SimpleStringRedisMessage.class, response);
        assertEquals(Response.OK, ((SimpleStringRedisMessage) response).content());
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("invalidArguments")
    void shouldRejectInvalidArguments(String name, List<String> rawArgs, String expectedError) {
        Object response = runRaw(channel, CommandType.MYCOMMAND, rawArgs);

        assertInstanceOf(ErrorRedisMessage.class, response);
        assertEquals(expectedError, ((ErrorRedisMessage) response).content());
    }
}
```

## Example Without FDB: SHOUT

A command without FDB work does not need `AsyncCommandExecutor` or a transaction. This `SHOUT` command returns its
argument in uppercase. You still need Steps 5 to 9 for it.

**1.** Add the enum entry to `CommandType`:

```java
SHOUT,
```

And add it to the `parse()` switch:

```java
case "SHOUT" -> SHOUT;
```

**2.** Protocol message, `ShoutMessage.java`:

```java
package com.kronotop.myfeature.handlers.protocol;

import com.kronotop.internal.ProtocolMessageUtil;
import com.kronotop.server.ProtocolMessage;
import com.kronotop.server.Request;

import java.util.List;

public class ShoutMessage implements ProtocolMessage<Void> {
    public static final String COMMAND = "SHOUT";
    public static final int MINIMUM_ARGUMENT_COUNT = 1;
    public static final int MAXIMUM_ARGUMENT_COUNT = 1;

    private String text;

    public ShoutMessage(Request request) {
        parse(request);
    }

    private void parse(Request request) {
        text = ProtocolMessageUtil.readAsString(request.getArguments().get(0));
    }

    public String getText() {
        return text;
    }

    @Override
    public Void getKey() {
        return null;
    }

    @Override
    public List<Void> getKeys() {
        return null;
    }
}
```

**3.** Add the attribute key to `MessageTypes`:

```java
public static final AttributeKey<ShoutMessage> SHOUT = AttributeKey.valueOf(ShoutMessage.COMMAND);
```

**4.** Handler, `ShoutHandler.java`:

```java
package com.kronotop.myfeature.handlers;

import com.kronotop.myfeature.handlers.protocol.ShoutMessage;
import com.kronotop.server.Handler;
import com.kronotop.server.MessageTypes;
import com.kronotop.server.Request;
import com.kronotop.server.Response;
import com.kronotop.server.annotation.Command;
import com.kronotop.server.annotation.MaximumArgumentCount;
import com.kronotop.server.annotation.MinimumArgumentCount;

@Command(ShoutMessage.COMMAND)
@MinimumArgumentCount(ShoutMessage.MINIMUM_ARGUMENT_COUNT)
@MaximumArgumentCount(ShoutMessage.MAXIMUM_ARGUMENT_COUNT)
public class ShoutHandler implements Handler {

    @Override
    public boolean isRedisCompatible() {
        return false;
    }

    @Override
    public void beforeExecute(Request request) {
        request.attr(MessageTypes.SHOUT).set(new ShoutMessage(request));
    }

    @Override
    public void execute(Request request, Response response) {
        ShoutMessage message = request.attr(MessageTypes.SHOUT).get();
        response.writeSimpleString(message.getText().toUpperCase());
    }
}
```

## Handler Interface Methods

| Method                            | Purpose                                                                                                                                   |
|-----------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------|
| `beforeExecute(Request)`          | Parse the arguments into a protocol message. Called before `execute()`.                                                                   |
| `execute(Request, Response)`      | Run the command and write the response.                                                                                                   |
| `requiresClusterInitialization()` | Return `false` if the command works before cluster init, as `PING` does. Default: `true`                                                  |
| `isWatchable()`                   | Return `true` if the command writes keys. After `execute()`, the keys from `getKeys()` are marked as modified for WATCH. Default: `false` |
| `isRedisCompatible()`             | Return `false` for Kronotop commands. They do not take the MULTI/EXEC lock and are rejected inside MULTI/EXEC. Default: `true`            |
| `getKeys(Request)`                | Return the keys the command writes. Used only when `isWatchable()` returns `true`. Default: empty list                                    |

## Annotations Reference

### @Command

Sets the command name that the handler processes.

```java
@Command("MYCOMMAND")
public class MyCommandHandler implements Handler { }
```

`@Command` is repeatable. For a handler that processes more than one command, add it more than once:

```java
@Command("SUBSTR")
@Command("GETRANGE")
public class GetRangeHandler implements Handler { }
```

### @MinimumArgumentCount

Sets the minimum number of arguments. The command name is not counted.

```java
@MinimumArgumentCount(1)
```

### @MaximumArgumentCount

Sets the maximum number of arguments. Count the optional keywords and their values too: `MYCOMMAND <key> [NAMESPACE
<path>]` has a maximum of 3.

```java
@MaximumArgumentCount(3)
```

Leave out this annotation for commands with no upper limit. If the count is wrong, the server returns
`ERR wrong number of arguments for '<command>' command`.

## Response Writing Methods

Common `Response` methods:

| Method                            | Output           |
|-----------------------------------|------------------|
| `writeOK()`                       | `+OK`            |
| `writeSimpleString(String)`       | `+PONG`          |
| `writeInteger(long)`              | `:42`            |
| `writeDouble(double)`             | `,3.14`          |
| `writeBigNumber(...)`             | `(1234...`       |
| `writeNULL()`                     | `_`              |
| `writeBoolean(boolean)`           | `#t` or `#f`     |
| `writeFullBulkString(...)`        | `$5\r\nhello`    |
| `writeVerbatimString(ByteBuf)`    | `=15\r\ntxt:...` |
| `writeArray(List<RedisMessage>)`  | `*2\r\n...`      |
| `writeSet(Set<RedisMessage>)`     | `~2\r\n...`      |
| `writeMap(Map<...>)`              | `%2\r\n...`      |
| `writeRedisMessage(RedisMessage)` | any              |
| `writeError(String)`              | `-ERR message`   |
| `writeBulkError(String)`          | `!21\r\nERR ...` |

Prefer throwing an exception to calling `writeError`. See Step 4.

## File Locations

| Component                       | Location                                                     |
|---------------------------------|--------------------------------------------------------------|
| Server CommandType enum         | `com.kronotop.server.CommandType`                            |
| Handler interface               | `com.kronotop.server.Handler`                                |
| MessageTypes                    | `com.kronotop.server.MessageTypes`                           |
| ProtocolMessage interface       | `com.kronotop.server.ProtocolMessage`                        |
| ProtocolMessageUtil             | `com.kronotop.internal.ProtocolMessageUtil`                  |
| Response interface              | `com.kronotop.server.Response`                               |
| Annotations                     | `com.kronotop.server.annotation.*`                           |
| CommandHandlerService           | `com.kronotop.CommandHandlerService`                         |
| AsyncCommandExecutor            | `com.kronotop.AsyncCommandExecutor`                          |
| TransactionUtil                 | `com.kronotop.transaction.TransactionUtil`                   |
| NamespaceUtil                   | `com.kronotop.namespace.NamespaceUtil`                       |
| KronotopException               | `com.kronotop.KronotopException`                             |
| IllegalCommandArgumentException | `com.kronotop.server.IllegalCommandArgumentException`        |
| Command metadata                | `kronotop/src/main/resources/commands/*.json`                |
| Client CommandType enum         | `kronotop-commands`: `com.kronotop.commands.CommandType`     |
| Command builders                | `kronotop-commands`: `com.kronotop.commands.*CommandBuilder` |
| Handler test base class         | `kronotop/src/test`: `com.kronotop.BaseHandlerTest`          |
| Reference handler (ZDEL)        | `com.kronotop.zmap.handlers.ZDelHandler`                     |
| Reference message (ZDEL)        | `com.kronotop.zmap.handlers.protocol.ZDelMessage`            |
| Reference test (ZDEL)           | `com.kronotop.zmap.handlers.ZDelHandlerTest`                 |
| Example handler (PING)          | `com.kronotop.core.handlers.connection.PingHandler`          |
