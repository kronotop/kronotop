---
title: "SESSION.ATTRIBUTE"
description: "Views and modifies session-specific configuration attributes."
---

Views and modifies session-specific configuration attributes.

## Syntax

```kronotop
SESSION.ATTRIBUTE LIST
SESSION.ATTRIBUTE SET <attribute> <value>
```

## Subcommands

### LIST

Returns all session attributes with their current values.

In RESP3 the response is a map; in RESP2 it is a flat array of alternating key-value pairs.

### SET

Sets a single session attribute to the given value. Returns `OK` on success.

## Attributes

| Attribute          | Type    | Default | Valid Values | Description                                       |
|--------------------|---------|---------|--------------|---------------------------------------------------|
| `reply_type`       | enum    | bson    | bson, json   | Data interchange format for responses             |
| `input_type`       | enum    | bson    | bson, json   | Data interchange format for inputs                |
| `limit`            | integer | 100     | > 0          | Maximum entries returned per query response       |
| `object_id_format` | enum    | bytes   | bytes, hex   | Encoding format for object ID values in responses |

All attribute names and enum values are case-insensitive.

## Errors

| Error                                      | Cause                                         |
|--------------------------------------------|-----------------------------------------------|
| `ERR Invalid subcommand status: <value>`   | The subcommand is neither `LIST` nor `SET`    |
| `ERR Invalid reply type: <value>`          | Invalid value for `reply_type`                |
| `ERR Invalid input type: <value>`          | Invalid value for `input_type`                |
| `ERR 'limit' must be greater than 0`       | `limit` was set to 0 or a negative number     |
| `ERR Invalid versionstamp format: <value>` | Invalid value for `object_id_format`          |
| `ERR invalid number of parameters`         | `SET` called without both attribute and value |
| `ERR Invalid session attribute: '<name>'`  | The attribute name does not exist             |

## Examples

**List all attributes:**

```kronotop
> SESSION.ATTRIBUTE LIST
1# reply_type => bson
2# input_type => bson
3# limit => (integer) 100
4# object_id_format => bytes
```

**Set the reply type to JSON:**

```kronotop
> SESSION.ATTRIBUTE SET reply_type JSON
OK
```

**Set limit:**

```kronotop
> SESSION.ATTRIBUTE SET limit 50
OK
```

**Invalid attribute name:**

```kronotop
> SESSION.ATTRIBUTE SET unknown_attr value
(error) ERR Invalid session attribute: 'unknown_attr'
```

**Invalid reply type value:**

```kronotop
> SESSION.ATTRIBUTE SET reply_type xml
(error) ERR Invalid reply type: xml
```
