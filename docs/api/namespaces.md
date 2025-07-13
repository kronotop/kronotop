# Namespaces

Namespace is a thin layer around FoundationDB's powerful [directory](https://apple.github.io/foundationdb/developer-guide.html#directories) concept. Namespaces are a
recommended approach for administering applications. Each application should create or open at least one directory to 
manage its subspaces. Namespaces are identified by hierarchical paths analogous to the paths in a Unix-like file system.

The hierarchy between directories is denoted by joining them with a dot(`.`) character. Let's assume that you have different
microservices and want to isolate their databases. You can create a namespace for each microservice under the
`production` namespace.

If you have three microservices named `users`, `orders` and `products`, your namespace hierarchy might look like the
following:

* `production.users`
* `production.orders`
* `production.products`

All data structures used by these namespaces will be isolated.

**Note:** Redis data structures are not stored under namespaces. Currently, namespaces cover only ZMap data structure.

## Commands

### NAMESPACE CREATE

`NAMESPACE CREATE` creates a new namespace with the given hierarchy.

**Syntax**

```
NAMESPACE CREATE <namespace-hierarchy>
```

**Example**

```
127.0.0.1:5484> NAMESPACE CREATE global.users 
```

### NAMESPACE REMOVE

`NAMESPACE REMOVE` removes the given namespace hierarchy. It's the equivalent of `DROP DATABASE` command in other
databases. So you should be careful when start typing this command. The result is irreversible.

**Syntax**

```
NAMESPACE REMOVE <namespace-hierarchy>
```

**Example**

```
127.0.0.1:5484> NAMESPACE REMOVE global.users
OK 
```

**Error Cases**

You cannot remove the default namespace:

```
127.0.0.1:5484> NAMESPACE REMOVE global
(error) ERR Cannot remove the default namespace: 'global'
```

### NAMESPACE CURRENT

`NAMESPACE CURRENT` command returns the current namespace in use for the session. The default namespace is in use if the
client doesn't set a different namespace by calling `NAMESPACE USE` command.

**Syntax**

```
NAMESPACE CURRENT
```

**Example**

```
127.0.0.1:5484> NAMESPACE CURRENT
global 
```

### NAMESPACE USE

`NAMESPACE USE` sets an existing namespace to be used by the session.

**Syntax**

```
NAMESPACE USE <namespace-hierarchy>
```

**Example**

```
127.0.0.1:5484> NAMESPACE USE global.users
OK 
```

**Error Cases**

It returns `NOSUCHNAMESPACE` error if the namespace doesn't exist:

```
127.0.0.1:5484> namespace use global.clients
(error) NOSUCHNAMESPACE No such namespace: global.clients
```

### NAMESPACE EXISTS

`NAMESPACE EXISTS` checks a namespace whether exists or not.

**Syntax**

```
NAMESPACE EXISTS <namespace-hierarchy>
```

**Example**

```
127.0.0.1:5484> NAMESPACE EXISTS global.users
(integer) 1
```

It returns `1` for existing namespaces and `0` for non-existing ones.

### NAMESPACE LIST

`NAMESPACE LIST` lists the namespaces under the given hierarchy.

**Syntax**

```
NAMESPACE LIST <namespace-hierarchy>
```

**Example**

```
127.0.0.1:5484> NAMESPACE LIST
1) global
```

It can take an optional parameter of namespace hierarchy.

```
127.0.0.1:5484> NAMESPACE LIST global
1) users
```

**Error Cases**

It returns `NOSUCHNAMESPACE` error if the given hierarchy is invalid:

```
127.0.0.1:5484> NAMESPACE LIST global.clients
(error) NOSUCHNAMESPACE No such namespace: global.clients
```

### NAMESPACE MOVE

`NAMESPACE MOVE` moves the rightmost namespace to another location in the given hierarchy.

**Syntax**

```
NAMESPACE MOVE <old-namespace-hierarchy> <new-namespace-hierarchy>
```

**Example**

```
127.0.0.1:5484> NAMESPACE MOVE global.users staging.users
OK
```

Let's check the result:

```
127.0.0.1:5484> NAMESPACE LIST global
(empty array)
127.0.0.1:5484> NAMESPACE LIST staging
1) users
```