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
| `batch`            | integer | 100     | > 0          | Number of documents returned per batch            |
| `object_id_format` | enum    | bytes   | bytes, hex   | Encoding format for object ID values in responses |

All attribute names and enum values are case-insensitive.

## Errors

Argument errors:

| Error Code | Error message                         | Cause |
|------------|---------------------------------------|-------|
| `ERR`      | `Unknown subcommand: '<value>'`       | -     |
| `ERR`      | `invalid number of parameters`        | -     |
| `ERR`      | `Unknown session attribute: '<name>'` | -     |
| `ERR`      | `Unknown reply type: '<value>'`       | -     |
| `ERR`      | `Unknown input type: '<value>'`       | -     |
| `ERR`      | `Unknown object id format: '<value>'` | -     |
| `ERR`      | `'batch' must be greater than 0`      | -     |

## Examples

**List all attributes:**

```kronotop
> SESSION.ATTRIBUTE LIST
1# reply_type => bson
2# input_type => bson
3# batch => (integer) 100
4# object_id_format => bytes
```

**Set the reply type to JSON:**

```kronotop
> SESSION.ATTRIBUTE SET reply_type JSON
OK
```

**Set batch:**

```kronotop
> SESSION.ATTRIBUTE SET batch 50
OK
```

**Invalid attribute name:**

```kronotop
> SESSION.ATTRIBUTE SET unknown_attr value
(error) ERR Unknown session attribute: 'unknown_attr'
```

**Invalid reply type value:**

```kronotop
> SESSION.ATTRIBUTE SET reply_type xml
(error) ERR Unknown reply type: 'xml'
```
