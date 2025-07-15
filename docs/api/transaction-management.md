# Transaction management

* [BEGIN](#begin)
* [COMMIT](#commit)
* [ROLLBACK](#rollback)
* [GETAPPROXIMATESIZE](#getapproximatesize)
* [GETREADVERSION](#getreadversion)
* [SNAPSHOTREAD](#snapshotread)

Kronotop exposes the FoundationDB API to Redis clients. This section explains transaction management commands.

**Note:** Not all parts of the FoundationDB API are implemented yet

## Commands

### BEGIN

`BEGIN` creates a new transaction and set it to the current session.

**Syntax**

```
BEGIN
```

**Example:**

```
127.0.0.1:5484> BEGIN
OK
```

**Error Cases**

If there is another transaction in progress in the current session, it returns a `TRANSACTION` error message.

```
127.0.0.1:5484> BEGIN
(error) TRANSACTION there is already a transaction in progress.
```

### COMMIT

`COMMIT` commits changes to the database.

**Syntax**

```
COMMIT
```

**Example:**

```
127.0.0.1:5484> COMMIT
OK
```

**Error Cases**

FoundationDB currently does not support transactions running for over five seconds. If the transaction is too old,
it returns `TRANSACTIONOLD` error.

```
127.0.0.1:5484> COMMIT
(error) TRANSACTIONOLD transaction is too old to perform reads or be committed
```

If there is no transaction in progress in the current session, it returns `TRANSACTION` error.

```
127.0.0.1:5484> COMMIT
(error) TRANSACTION there is no transaction in progress.
```

### ROLLBACK

`ROLLBACK` cancels the current session's in-progress transaction.

**Syntax**

```
ROLLBACK
```

**Example:**

```
127.0.0.1:5484> ROLLBACK
OK
```

**Error Cases**

If no transaction is in progress in the current session, it returns a `TRANSACTION` error message.

```
127.0.0.1:5484> ROLLBACK
(error) TRANSACTION there is no transaction in progress.
```

### GETAPPROXIMATESIZE

`GETAPPROXIMATESIZE` returns the approximated size of the commit, which is the summation of mutations, read conflict
ranges,
and write conflict ranges. This can be called multiple times before a transaction commit.

**Syntax**

```
GETAPPROXIMATESIZE
```

**Example**

```
127.0.0.1:5484> GETAPPROXIMATESIZE
(integer) 619
```

### GETREADVERSION

`GETREADVERSION` returns the version at which the reads for the in-progress transaction will access the database.

**Syntax**

```
GETREADVERSION
```

**Example**

```
127.0.0.1:5484> GETREADVERSION
16275608010704
```

### SNAPSHOTREAD

`SNAPSHOTREAD` enables or disables snapshot reads for the in-progess transaction.

In the FoundationDB context, snapshots are special-purpose, read-only view of the database. Reads done through this
interface are known as "snapshot reads." Snapshot reads selectively relax FoundationDB's isolation property, reducing [Transaction
conflicts](https://apple.github.io/foundationdb/developer-guide.html#conflict-ranges) but making reasoning about concurrency harder. For more information about how to use snapshot reads correctly,
see Using [snapshot reads](https://apple.github.io/foundationdb/developer-guide.html#snapshot-reads).

**Syntax**

```
SNAPSHOTREAD <argument>
```

**Arguments**

* `ON`
* `OFF`

**Example:**

```
127.0.0.1:5484> SNAPSHOTREAD ON
OK
```