#  KVStore (Key/Value Store)

## Overview 

Package KVStore provides implementations of thread-safe Go data structures that
are backed by BadgerDB. Users don't directly interact with the data structures,
but rather use the exposed thread-safe methods.

## Supported Data Structures 

### MapStore 

Currently the only structure is a MapStore represented in memory as a Go
`map[string]map[string]T` where `T` is any user type that supports (or
implements support) for being encoded/decoded using
[encoding/gob](https://pkg.go.dev/encoding/gob). The key for the outer map is
used as the Badger key and the inner map is stored as the value. This allows
multiple instances of `T` to be stored for a particular entry in Badger.

To setup a new instance of a map store: 

```go
mapStoreOpts := badger.DefaultOptions("/tmp/mymapstore")
// Optional if you wish to log from BadgerDB using Zap:
mapStoreOpts = jobStoreOpts.WithLogger(logger.NewBadgerLoggerBridge("mapStore", log))
// Replace 'string' with whatever type you wish to store in the map:
mapStore, closeMapStore, err := kvstore.NewMapStore[string](jobStoreOpts, m.config.JobStoreCacheSize, testMode)
if err != nil {
    log.Error("unable to open map store")
}
```
When you are finished with the map store it must be closed, usually with a defer where it is important to log any errors if needed:
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
// First get an entry:
entry, commit, _ := mapStore.CreateAndLockEntry("foo")
// Each entry is a map, so it can be accessed like:
entry.Value["one"] = "1"
entry.Value["two"] = "2"
// It is also possible to store arbitrary metadata along with the value.
// This is useful if you wish to lookup an associated entry in some other MapStore:
entry.Metadata["hello"] = "world"
// When you are finished commit the results: 
_ = commit()
// IMPORTANT: Behavior is indeterminate if the entry is used after this point.
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

While the cache is updated as soon as the `entry.Value` map is updated, the cache is not synced with BadgerDB until `commit()` is called. If you don't need write access to the entry, you can get read only access to the latest version of the entry in BadgerDB with the `GetEntry` and `GetEntries` methods. This allows a single writer to periodically make updates to an entry and multiple readers to query the latest stable status of the entry.

#### Known Issues and Limitations 

[It is possible for the caller to continue using their pointer to an entry after they call `commit`.](https://github.com/ThinkParQ/gobee/issues/10)
