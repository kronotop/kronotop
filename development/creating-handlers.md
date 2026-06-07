# Creating Command Handlers

This guide explains how to create new command handlers in Kronotop.

## Overview

Kronotop uses a handler-based architecture for processing RESP protocol commands. Each command is processed by a handler
class that implements the `Handler` interface. Handlers are registered with a service and invoked when matching commands
arrive.

## Quick Start Checklist

1. Add command to `CommandType` enum
2. Create protocol message class
3. Add `AttributeKey` to `MessageTypes`
4. Create handler class with annotations
5. Register handler in service

## Step-by-Step Guide

### Step 1: Add Command to CommandType Enum

Add your command to the `CommandType` enum in `com.kronotop.server.CommandType`:

```java
public enum CommandType {
    // ... existing commands ...

    // Your new command
    MYCOMMAND,

    // For commands with dots (e.g., MY.COMMAND):
    MY_COMMAND("MY.COMMAND"),

    // ... rest of enum ...
}
```

Update the `parse()` method's switch statement:

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

### Step 2: Create Protocol Message Class

Create a message class that parses the command parameters. Place it in a `protocol` subpackage within your feature
package.

**Example: `MyCommandMessage.java`**

```java
package com.kronotop.myfeature.handlers.protocol;

import com.kronotop.server.ProtocolMessage;
import com.kronotop.server.Request;
import io.netty.util.CharsetUtil;

import java.util.List;

public class MyCommandMessage implements ProtocolMessage<String> {
    public static final String COMMAND = "MYCOMMAND";
    public static final int MINIMUM_PARAMETER_COUNT = 1;
    public static final int MAXIMUM_PARAMETER_COUNT = 2;

    private final Request request;
    private String key;
    private String optionalValue;

    public MyCommandMessage(Request request) {
        this.request = request;
        parse();
    }

    private void parse() {
        key = request.getParams().get(0).toString(CharsetUtil.UTF_8);
        if (request.getParams().size() > 1) {
            optionalValue = request.getParams().get(1).toString(CharsetUtil.UTF_8);
        }
    }

    public String getOptionalValue() {
        return optionalValue;
    }

    @Override
    public String getKey() {
        return key;
    }

    @Override
    public List<String> getKeys() {
        return List.of(key);
    }
}
```

### Step 3: Add AttributeKey to MessageTypes

Register your message type in `com.kronotop.server.MessageTypes`:

```java
public class MessageTypes {
    // ... existing types ...

    public static final AttributeKey<MyCommandMessage> MYCOMMAND =
            AttributeKey.valueOf(MyCommandMessage.COMMAND);
}
```

### Step 4: Create Handler Class

Create a handler class that implements `Handler` and uses annotations for configuration.

**Example: `MyCommandHandler.java`**

```java
package com.kronotop.myfeature.handlers;

import com.kronotop.myfeature.handlers.protocol.MyCommandMessage;
import com.kronotop.server.Handler;
import com.kronotop.server.MessageTypes;
import com.kronotop.server.Request;
import com.kronotop.server.Response;
import com.kronotop.server.annotation.Command;
import com.kronotop.server.annotation.MaximumParameterCount;
import com.kronotop.server.annotation.MinimumParameterCount;

@Command(MyCommandMessage.COMMAND)
@MinimumParameterCount(MyCommandMessage.MINIMUM_PARAMETER_COUNT)
@MaximumParameterCount(MyCommandMessage.MAXIMUM_PARAMETER_COUNT)
public class MyCommandHandler implements Handler {

    @Override
    public void beforeExecute(Request request) {
        request.attr(MessageTypes.MYCOMMAND).set(new MyCommandMessage(request));
    }

    @Override
    public void execute(Request request, Response response) throws Exception {
        MyCommandMessage message = request.attr(MessageTypes.MYCOMMAND).get();

        // Your command logic here
        String result = processCommand(message);

        response.writeSimpleString(result);
    }

    private String processCommand(MyCommandMessage message) {
        // Implementation
        return "OK";
    }
}
```

### Step 5: Register Handler in Service

Register your handler in the appropriate service class (e.g., `StashService`):

```java
public class MyService extends CommandHandlerService {

    public MyService(Context context) throws CommandAlreadyRegisteredException {
        super(context, NAME);

        // Register for external client connections
        handlerMethod(ServerKind.EXTERNAL, new MyCommandHandler());

        // Optionally register for internal cluster connections
        handlerMethod(ServerKind.INTERNAL, new MyCommandHandler());
    }
}
```

## Complete Example: Custom ECHO Command

Here's a complete example of a simple `SHOUT` command that echoes text in uppercase.

**1. CommandType.java** - Add enum entry:

```java
SHOUT,
```

And in the `parse()` switch:

```java
case "SHOUT" -> SHOUT;
```

**2. ShoutMessage.java** - Protocol message:

```java
package com.kronotop.myfeature.handlers.protocol;

import com.kronotop.server.ProtocolMessage;
import com.kronotop.server.Request;
import io.netty.util.CharsetUtil;

import java.util.List;

public class ShoutMessage implements ProtocolMessage<Void> {
    public static final String COMMAND = "SHOUT";
    public static final int MINIMUM_PARAMETER_COUNT = 1;
    public static final int MAXIMUM_PARAMETER_COUNT = 1;

    private String text;

    public ShoutMessage(Request request) {
        parse(request);
    }

    private void parse(Request request) {
        text = request.getParams().get(0).toString(CharsetUtil.UTF_8);
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

**3. MessageTypes.java** - Add attribute key:

```java
public static final AttributeKey<ShoutMessage> SHOUT =
        AttributeKey.valueOf(ShoutMessage.COMMAND);
```

**4. ShoutHandler.java** - Handler implementation:

```java
package com.kronotop.myfeature.handlers;

import com.kronotop.myfeature.handlers.protocol.ShoutMessage;
import com.kronotop.server.Handler;
import com.kronotop.server.MessageTypes;
import com.kronotop.server.Request;
import com.kronotop.server.Response;
import com.kronotop.server.annotation.Command;
import com.kronotop.server.annotation.MaximumParameterCount;
import com.kronotop.server.annotation.MinimumParameterCount;

@Command(ShoutMessage.COMMAND)
@MinimumParameterCount(ShoutMessage.MINIMUM_PARAMETER_COUNT)
@MaximumParameterCount(ShoutMessage.MAXIMUM_PARAMETER_COUNT)
public class ShoutHandler implements Handler {

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

| Method                            | Purpose                                                                               |
|-----------------------------------|---------------------------------------------------------------------------------------|
| `beforeExecute(Request)`          | Parse request parameters into a protocol message. Called before `execute()`.          |
| `execute(Request, Response)`      | Process the command and write the response.                                           |
| `requiresClusterInitialization()` | Return `false` if the command works before cluster init (e.g., PING). Default: `true` |
| `isWatchable()`                   | Return `true` if the command can be used with WATCH transactions. Default: `false`    |
| `isStashCompatible()`             | Return `true` if compatible with Stash commands. Default: `true`                      |
| `getKeys(Request)`                | Return keys accessed by this command (for routing). Default: empty list               |

## Annotations Reference

### @Command

Specifies the command name that this handler processes.

```java
@Command("MYCOMMAND")
public class MyCommandHandler implements Handler { }
```

For handlers that process multiple commands, use `@Commands`:

```java
@Command("SUBSTR")
@Command("GETRANGE")
public class GetRangeHandler implements Handler { }
```

### @MinimumParameterCount

Specifies the minimum number of parameters required (excluding the command name).

```java
@MinimumParameterCount(1)  // At least 1 parameter required
```

### @MaximumParameterCount

Specifies the maximum number of parameters allowed.

```java
@MaximumParameterCount(2)  // At most 2 parameters allowed
```

Omit this annotation for commands with unlimited parameters.

## Response Writing Methods

Common `Response` methods for writing results:

| Method                           | Example Output | Use Case               |
|----------------------------------|----------------|------------------------|
| `writeOK()`                      | `+OK`          | Success confirmation   |
| `writeSimpleString(String)`      | `+PONG`        | Simple status strings  |
| `writeInteger(long)`             | `:42`          | Numeric results        |
| `writeDouble(double)`            | `,3.14`        | Floating-point results |
| `writeNULL()`                    | `_`            | Null/not found         |
| `writeBoolean(boolean)`          | `#t` or `#f`   | Boolean results        |
| `writeArray(List<RedisMessage>)` | `*2\r\n...`    | Lists of values        |
| `writeMap(Map<...>)`             | `%2\r\n...`    | Key-value maps         |
| `writeError(String)`             | `-ERR message` | Error responses        |
| `writeFullBulkString(...)`       | `$5\r\nhello`  | Binary-safe strings    |

## File Locations

| Component                 | Location                                                      |
|---------------------------|---------------------------------------------------------------|
| CommandType enum          | `com.kronotop.server.CommandType`                             |
| Handler interface         | `com.kronotop.server.Handler`                                 |
| MessageTypes              | `com.kronotop.server.MessageTypes`                            |
| ProtocolMessage interface | `com.kronotop.server.ProtocolMessage`                         |
| Response interface        | `com.kronotop.server.Response`                                |
| Annotations               | `com.kronotop.server.annotation.*`                            |
| CommandHandlerService     | `com.kronotop.CommandHandlerService`                          |
| Example handler (PING)    | `com.kronotop.stash.handlers.connection.PingHandler`          |
| Example message (PING)    | `com.kronotop.stash.handlers.connection.protocol.PingMessage` |
