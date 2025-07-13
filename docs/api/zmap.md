### ZMap

* [Ordered Key-Value Store Backed by FoundationDB](#ordered-key-value-store-backed-by-foundationdb)
* [One-Off Transactions](#one-off-transactions)
* [ZMap and Namespaces](#zmap-and-namespaces)
* [Commands](#commands)
  * [ZSET](#zset)
  * [ZGET](#zget)
  * [ZGETKEY](#zgetkey)
  * [ZGETRANGE](#zgetrange)
  * [ZGETRANGESIZE](#zgetrangesize)
  * [ZMUTATE](#zmutate)
  * [ZDEL](#zdel)
  * [ZDELRANGE](#zdelrange)

## Ordered Key-Value Store Backed by FoundationDB

Kronotop introduces a novel data structure called **ZMap**, which serves as a Redis-compatible proxy over **FoundationDB’s 
transactional key-value API**.

Under the hood, ZMap leverages **FoundationDB’s core data model** — a highly reliable, ordered key-value store.  
Also referred to as an **ordered associative array** or dictionary, this structure is composed of unique keys arranged in 
lexicographical order, each mapped to a corresponding value.

By exposing this powerful model through the familiar **RESP protocol**, ZMap allows developers to interact with 
FoundationDB in a simple, efficient, and idiomatic way — without requiring deep knowledge of FoundationDB's lower-level API.

ZMap forms the foundation for building richer abstractions and serves as a building block for higher-level features 
like **namespaces**, **Buckets**, and transactional queries through **MQL**.

See the [Data Modeling](https://apple.github.io/foundationdb/data-modeling.html) section on FoundationDB documents.

## One-Off Transactions

Kronotop will create a new transaction and automatically commit changes to the database for the ad-hoc commands.

In other terms, you do not need to do this for every command:

```
127.0.0.1:5484> BEGIN
OK
127.0.0.1:5484> ZSET mykey "Hello"
OK
127.0.0.1:5484> COMMIT
OK
```

The one-off transaction feature is especially useful for running commands in CLI.

## ZMap and Namespaces

A namespace prefixes all data stored in a ZMap. The current namespace can be inspected by running the following command:

```
127.0.0.1:5484> NAMESPACE CURRENT
global
```

`global` is the default namespace. Namespaces create isolation between data structures. This is how it works for ZMap

```
127.0.0.1:5484> NAMESPACE CURRENT
global
127.0.0.1:5484> ZSET mykey "Hello"
OK
127.0.0.1:5484> ZGET mykey
"Hello"
127.0.0.1:5484> NAMESPACE CREATE global.child-namespace
OK
127.0.0.1:5484> NAMESPACE USE global.child-namespace
OK
127.0.0.1:5484> ZGET mykey
(nil)
```

See the [Namespaces](namespaces.md) section for further information.

## Commands

### ZSET

`ZSET` sets the value for a given key. This will not affect the database until `COMMIT` is called.

**Syntax**

```
ZSET key value
```

**Example:**

```
127.0.0.1:5484> ZSET mykey "Hello"
OK
```

### ZGET

`ZGET` gets a value from the database. The call will return `nil` if the key is not present in the database.

**Syntax**

```
ZGET key
```

**Example:**

```
127.0.0.1:5484> ZGET mykey
"Hello"
```

### ZGETKEY

`ZGETKEY` returns the key referenced by the specified `KeySelector`.

The default key selector is `FIRST_GREATER_OR_EQUAL`. `KEY_SELECTOR` portion of the command is optional.

**Syntax**

```
ZGETKEY key value [KEY_SELECTOR selector]
```

Available key selectors:

* `FIRST_GREATER_OR_EQUAL`,
* `FIRST_GREATER_THAN`,
* `LAST_LESS_THAN`,
* `LAST_LESS_OR_EQUAL`

**Example:**

```
127.0.0.1:5484> zset key-0 value-0
OK
127.0.0.1:5484> zset key-1 value-1
OK
127.0.0.1:5484> zset key-2 value-2
OK
127.0.0.1:5484> zset key-3 value-3
OK
127.0.0.1:5484> ZGETKEY key-0 KEY_SELECTOR FIRST_GREATER_THAN
"key-1"
```

### ZGETRANGE

`ZGETRANGE` gets an ordered range of keys and values from the database. The *begin* and *end* keys can be specified by
key selectors, with the begin `BEGIN_KEY_SELECTOR` inclusive and the end `END_KEY_SELECTOR` exclusive.

**Syntax**

```
ZGETRANGE key [key ...] [BEGIN_KEY_SELECTOR selector] [END_KEY_SELECTOR selector] [LIMIT limit] [REVERSE]
```

The default `BEGIN_KEY_SELECTOR` is `FIRST_GREATER_OR_EQUAL`.
The default `END_KEY_SELECTOR` is `FIRST_GREATER_THAN`.

Available key selectors:

* `FIRST_GREATER_OR_EQUAL`,
* `FIRST_GREATER_THAN`,
* `LAST_LESS_THAN`,
* `LAST_LESS_OR_EQUAL`

The key selector section of this command is optional.

**Example:***

```
127.0.0.1:5484> ZGETRANGE key-0 key-3
1) 1) "key-0"
   2) "value-0"
2) 1) "key-1"
   2) "value-1"
3) 1) "key-2"
   2) "value-2"
```

With the optional `LIMIT` argument:

```
127.0.0.1:5484> ZGETRANGE key-0 key-5 LIMIT 3
1) 1) "key-0"
   2) "value-0"
2) 1) "key-1"
   2) "value-1"
3) 1) "key-2"
   2) "value-2"
```

With `LIMIT` and `REVERSE` arguments:

```
127.0.0.1:5484> ZGETRANGE key-0 key-5 LIMIT 3 REVERSE
1) 1) "key-4"
   2) "value-4"
2) 1) "key-3"
   2) "value-3"
3) 1) "key-2"
   2) "value-2"
```

The `REVERSE` argument is also optional.

Range from beginning to end, get an ordered set or key/value pairs:

```
127.0.0.1:5484> ZGETRANGE * *
1) 1) "key-0"
   2) "value-0"
2) 1) "key-1"
   2) "value-1"
3) 1) "key-2"
   2) "value-2"
4) 1) "key-3"
   2) "value-3"
5) 1) "key-4"
   2) "value-4"
6) 1) "key-5"
   2) "value-5"
7) 1) "key-6"
   2) "value-6"
```

Explicitly using key selectors, please note that you can use the key selectors individually.

```
127.0.0.1:5484> ZGETRANGE key-2 key-5 BEGIN_KEY_SELECTOR FIRST_GREATER_THAN
1) 1) "key-3"
   2) "value-3"
2) 1) "key-4"
   2) "value-4"
```

### ZGETRANGESIZE

`ZGETRANGESIZE` gets an estimate for the number of bytes stored in the given range.

**Syntax**

```
ZGETRANGESIZE begin-key end-key
```

**Example**

```
127.0.0.1:5484> zgetrangesize key-0 key-9
(integer) 0
```

_FoundationDB says:_

> Note: the estimated size is calculated based on the sampling done by FDB server. The sampling algorithm works roughly
> in
> this way: the larger the key-value pair is, the more likely it would be sampled and the more accurate its sampled size
> would be.
> And due to that reason, it is recommended to use this API to query against large ranges for accuracy considerations.
> For a rough reference, if the returned size is larger than 3MB, one can consider the size to be accurate.

### ZMUTATE

`ZMUTATE` runs an atomic operation on the given key.

**Syntax**

```
ZMUTATE key value mutation-type
```

**Valid Mutation Types**

* `ADD`: Performs an addition of little-endian integers.
* `APPEND_IF_FITS`: Appends param to the end of the existing value already in the database at the given key (or creates
  the key and sets the value to param if the key is empty).
* `BIT_AND`: Performs a bitwise and operation.
* `BIT_OR`: Performs a bitwise or operation.
* `BIT_XOR`: Performs a bitwise xor operation.
* `BYTE_MAX`: Performs lexicographic comparison of byte strings.
* `BYTE_MIN`: Performs lexicographic comparison of byte strings.
* `COMPARE_AND_CLEAR`: Performs an atomic compare and clear operation.
* `MAX`: Performs a little-endian comparison of byte strings.
* `MIN`: Performs a little-endian comparison of byte strings.
* `SET_VERSIONSTAMPED_KEY`: Transforms key using a versionstamp for the transaction.
* `SET_VERSIONSTAMPED_VALUE`: Transforms param using a versionstamp for the transaction.

**Example:**

```
127.0.0.1:5484> ZSET key-0 value-0
OK
127.0.0.1:5484> ZMUTATE key-0 value COMPARE_AND_CLEAR
OK
127.0.0.1:5484> ZGET key-0
"value-0"
127.0.0.1:5484> ZMUTATE key-0 value-0 COMPARE_AND_CLEAR
OK
127.0.0.1:5484> ZGET key-0
(nil)
```

### ZDEL

`ZDEL` clears a given key from the database. This will not affect the database until `COMMIT` is called.

**Syntax**

```
ZDEL key
```

**Example:**

```
127.0.0.1:5484> ZDEL mykey
OK
```

### ZDELRANGE

`ZDELRANGE` clears the keys in the given range. The *begin* is inclusive, *end* is exclusive.

**Syntax**

```
ZDELRANGE begin-key end-key
```

**Example:**

```
127.0.0.1:5484> ZDELRANGE key-1 key-4
OK
127.0.0.1:5484> ZGETRANGE * *
1) 1) "key-0"
   2) "value-0"
2) 1) "key-4"
   2) "value-4"
3) 1) "key-5"
   2) "value-5"
4) 1) "key-6"
   2) "value-6"
```

Using `*` asterisk character to set a boundary:

Wipe out all keys in the current namespace:

```
127.0.0.1:5484> ZDELRANGE * *
OK
127.0.0.1:5484> ZGETRANGE * *
(empty array)
```

Clear all keys from the beginning to `key-3`:

```
127.0.0.1:5484> ZDELRANGE * key-3
OK
127.0.0.1:5484> ZGETRANGE * *
1) 1) "key-3"
   2) "value-3"
2) 1) "key-4"
   2) "value-4"
3) 1) "key-5"
   2) "value-5"
```

Clear all keys from `key-3` to the end of the range:

```
127.0.0.1:5484> ZDELRANGE key-3 *
OK
127.0.0.1:5484> ZGETRANGE * *
1) 1) "key-0"
   2) "value-0"
2) 1) "key-1"
   2) "value-1"
3) 1) "key-2"
   2) "value-2"
```
