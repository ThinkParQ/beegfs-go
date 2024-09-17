#  KVStore (Key/Value Store)

## Overview 

Package KVStore provides implementations of thread-safe Go data structures that are backed by BadgerDB. Users don't
directly interact with the data structures, but rather use the exposed thread-safe methods.

## Supported Data Structures 

### MapStore 

MapStore offers a thread-safe key-to-entry mapping, where keys are strings and entries are any concrete type that can
be encoded and decoded using the [encoding/gob](https://pkg.go.dev/encoding/gob) package.

When creating a new entry of a reference type that requires initialization (such as a map or slice), you must initialize
it before it can be used. This can be done using one of the following options.
```go
key, entry, commit, err := mapStore.CreateAndLockEntry("key1", WithValue(make(map[string]string)))
// commit/error handling omitted...
```
```go
entry, commit, err := mapStore.CreateAndLockEntry("key1")
// commit/error handling omitted...
entry.Value = make(map[string]string)
```

#### Quick Start 

To setup a new instance of a map store: 

```go
mapStoreOpts := badger.DefaultOptions("/tmp/mymapstore")
// Optional if you wish to log from BadgerDB using Zap:
mapStoreOpts = jobStoreOpts.WithLogger(logger.NewBadgerLoggerBridge("mapStore", log))
// Replace 'map[string]string' with whatever type you wish to store in the map:
mapStore, closeMapStore, err := kvstore.NewMapStore[map[string]string](mapStoreOpts)
if err != nil {
    log.Error("unable to open map store")
}
```
When you are finished with the map store it must be closed, usually with a defer where it is important to log any errors
if needed:
```go
defer func() {
    err := closeMapStore()
    if err != nil {
        log.Error("unable to close map store", zap.Error(err))
    }
}
```

*From here examples omit error handling, but make sure to check errors throughout.*

To add an entry:
```go
// First create an entry:
_, entry, commit, _ := mapStore.CreateAndLockEntry("foo", WithValue(make(map[string]string)))
// Each entry is a map, so it can be accessed like:
entry.Value["one"] = "1"
entry.Value["two"] = "2"
// When you are finished commit the results: 
_ = commit()
```

To get an existing entry: 
```go 
entry, commit, _ := mapStore.GetAndLockEntry("foo")
one, ok := entry.Value["one"]
if ok {
   fmt.Println(one) 
}
_ = commit()
```

To get a created or retrieved an existing entry:
```go
_, entry, commit, _ := mapStore.CreateAndLockEntry(
    "foo",
    kvstore.WithAllowExisting(true), // Allow existing database entry to be returned.
    kvstore.WithValue(make(map[string]string)),
)
entry.Value["one"] = "1"
entry.Value["two"] = "2"
// When you are finished commit the results:
_ = commit()
```

"When calling `commit()` on an entry, you can specify one or more `commitEntryConfigOpts` to modify how the entry is
committed:

* To delete the entry immediately instead of updating the database, use the `WithDeleteEntry` option. This is useful if
  you want to avoid committing the entry and then deleting it in a separate transaction:

    ```go
    err := commit(WithDeleteEntry(true))
    ```

* If you want to update the entry (essentially flushing the cache to BadgerDB) but continue holding the lock on the
  entry, use:

    ```go
    err := commit(WithUpdateOnly(true))
    ```

#### Known Issues and Limitations 
