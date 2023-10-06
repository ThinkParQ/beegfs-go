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

