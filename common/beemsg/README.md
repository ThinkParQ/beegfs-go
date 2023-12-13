# BeeGFS Message communication utility, message definition and serializer

These packages provides utilities to communicate with BeeGFS nodes using the "old" way, called BeeMsg. These include:
* The message (de-)serializer
* Message definitions (under `msg/`)
* Communication functions for TCP and UDP
* A node store that maps node ids and other stuff to nodes and stores and reuses connections

The connection code is currently only suitable for "oneshot" use, e.g. a command line tool.

## Adding messages
Put new messages into the `msg/` package. Since the whole folder is one Go package, it doesn't
matter where exactly they go, just split them reasonably.

To add a message, add a struct named like `DoStuff`, add the appropriate fields and implement the
`SerializableMsg` and `DeserializableMsg`. If only one direction is needed, the other might be
omitted. Both require providing a `MsgId` function that returns the messages Id as well as the
(de)serialization function. Take a look at the existing definitions to see how it looks like.

**IMPORTANT:** The message definitions including the message ID and the (de-)serialization procedure must
match their counterpart in other languages. Currently these are:

* common code in C++ (usually found under `projects0/common/source/common/net/message/`)
* client code in C (usually found under `projects0/client_module/source/common/net/message/`)
* Rust code (`beegfs-rs/shared/msg`)

If they differ, problems ranging from not recognizing message to corrupted data will occur. This
cannot be checked automatically, so when you add new messages or edit existing ones or review these
changes, make sure you take that into account.
