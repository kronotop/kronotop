# Session Management

Kronotop provides a bunch of commands to manage database sessions.

* [Attributes](#attributes)
* [Configuration](#configuration)
* [Commands](#commands)
  * [SESSION.ATTRIBUTE LIST](#sessionattribute-list)
  * [SESSION.ATTRIBUTE SET](#sessionattribute-set)

## Attributes

The following attributes are set by default:

| attribute        | type    | scope  | description                                                              | default | available values |
|------------------|---------|--------|--------------------------------------------------------------------------|---------|------------------|
| reply_type       | enum    | Bucket | Data interchange format for the replies                                  | BSON    | BSON, JSON       |
| input_type       | enum    | Bucket | Data interchange format for the inputs                                   | BSON    | BSON, JSON       |
| limit            | integer | Bucket | Maximum entries returned per query response                              | 100     |                  |
| pin_read_version | boolean | Bucket | Reuse the initial read version for all subsequent `BUCKET.ADVANCE` calls | true    | true, false      |

## Configuration

You can override the default values by specifying new settings in the configuration file. Configuration options related to 
session behavior are grouped under the `session_attributes` object.

```hocon
  session_attributes {
    input_type = "bson"
    reply_type = "bson"
    limit = 100
    pin_read_version = true
  }

```
## Commands

Session management commands have been implemented as subcommands of `SESSION.ATTRIBUTE` command.

### SESSION.ATTRIBUTE LIST

`SESSION.ATTRIBUTE LIST` lists all attributes with their current values used by the session.

**Syntax**

```
SESSION.ATTRIBUTE LIST
```

**Example**

```
127.0.0.1:5484> SESSION.ATTRIBUTE LIST
1# reply_type => bson
2# input_type => bson
3# limit => (integer) 100
4# pin_read_version => (true)
```

#### SESSION.ATTRIBUTE SET

`SESSION.ATTRIBUTE SET` sets a value to an attribute.

**Syntax**

```
SESSION.ATTRIBUTE SET <attribute> <value>
```

**Example**

```
127.0.0.1:5484> SESSION.ATTRIBUTE SET reply_type JSON
OK
```

**Error Cases**

A random value cannot be set to an attribute, if its type is `enum`.

```
127.0.0.1:5484> SESSION.ATTRIBUTE SET reply_type some-value
(error) ERR Invalid reply type: some-value
```

It's not possible to set an attribute if it's not defined by Kronotop:

```
127.0.0.1:5484> SESSION.ATTRIBUTE set some-attribute value
(error) ERR Invalid session attribute: 'some-attribute'
```