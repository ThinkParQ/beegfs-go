BeeGFS Watch <!-- omit in toc -->
========

# Contents <!-- omit in toc -->

- [About](#about)
- [Quick Start](#quick-start)
  - [Using OS Packages](#using-os-packages)
  - [Using Container Images](#using-container-images)
  - [For Developers](#for-developers)
- [Configuring Watch](#configuring-watch)
  - [Overview](#overview)
    - [Specify Configuration Using Flags](#specify-configuration-using-flags)
    - [Specify Configuration Using Environment Variables](#specify-configuration-using-environment-variables)
    - [Specify Configuration using a TOML Configuration File](#specify-configuration-using-a-toml-configuration-file)
  - [Configuring Subscribers](#configuring-subscribers)
    - [gRPC](#grpc)
      - [OPTION 1: Specify the path to a self-signed certificate](#option-1-specify-the-path-to-a-self-signed-certificate)
      - [OPTION 2: Add subscriber certificate(s) to the system-wide store of trusted certificates on the Watch server](#option-2-add-subscriber-certificates-to-the-system-wide-store-of-trusted-certificates-on-the-watch-server)
      - [OPTION 3: Disable TLS (not recommended)](#option-3-disable-tls-not-recommended)
  - [Updating Configuration (without restarting Watch)](#updating-configuration-without-restarting-watch)
- [Shutting Down Watch](#shutting-down-watch)
- [Advanced](#advanced)
  - [Developing gRPC Subscribers](#developing-grpc-subscribers)
    - [Implementation Details](#implementation-details)
    - [Best Practices](#best-practices)
      - [Avoid dropped events by acknowledging events after they're processed](#avoid-dropped-events-by-acknowledging-events-after-theyre-processed)
      - [Optimize performance by not acknowledging every event](#optimize-performance-by-not-acknowledging-every-event)
      - [Avoid duplicate events by acknowledging the last event received when reconnecting](#avoid-duplicate-events-by-acknowledging-the-last-event-received-when-reconnecting)

# About

Watch is a replacement for the
[beegfs-event-listener](https://doc.beegfs.io/latest/advanced_topics/filesystem_modification_events.html#beegfs-event-listener).
It handles receiving file system modification events from a single BeeGFS
metadata service over a Unix socket then forwarding them to one or more
subscribers. Currently Watch only supports gRPC subscribers, but the design
is flexible so additional subscriber types can be easily added in the future
such as InfluxDB. 

Watch is responsible for avoiding lost events due to network or other remote
issues, for example if the server a subscriber is on reboots for updates. It
does so by buffering each event received from the BeeGFS metadata service
in-memory until the event is sent to all subscribers, or its internal event
buffer overflows (then the oldest event(s) will be dropped). It is not
responsible for avoiding lost events due to the server it runs on
rebooting/crashing for any reason, this will instead be handled by future
changes to the BeeGFS metadata service to add an on-disk event buffer that will
be used to recreate the Watch in-memory buffer after a planned/unplanned
restart. When supported by subscribers it can also guarantee a particular event
is only ever sent to a subscriber once.

# Quick Start

## Using OS Packages

The following steps walk through installing and configuring Watch using an
Debian or RPM package. By default when installed using a package Watch will
use the default configuration file installed at `/etc/beegfs/beegfs-watch.toml`
that will setup Watch to log to a file at `/var/log/beegfs/beegfs-watch.log` and
listen for Metadata events at `/run/beegfs/eventlog`. This section will use the
default configuration to initially start Watch, then show how to configure
subscribers without restarting Watch:

1. On the BeeGFS metadata server where you want to use Watch to forward file
   system modification events, download the appropriate package for your Linux
   distribution. Eventually packages will be made available through the BeeGFS
   package repositories but until then there are two options to use a package:
   1. Pre-built packages are available under
      [releases](https://github.com/ThinkParQ/beegfs-go/releases).
   2. Clone the GitHub repository and [build the packages yourself](build/README.md).

2. Install the selected package using your package manager. For example on
   Ubuntu run: `sudo dpkg -i beegfs-watch-8.0.0-alpha.1-linux.amd64.deb`.

3. Start the Watch service with: `systemctl start beegfs-watch`.

4. Update the BeeGFS Metadata configuration so `sysFileEventLogTarget` points at
   the same path as the Watch `event-log-target` setting (by default
   `/run/beegfs/eventlog`) then start/restart the Metadata service.
    * Note the parent directory of `sysFileEventLogTarget` must be accessible by
    the user executing Watch.

5. On all BeeGFS clients configure the `sysFileEventLogMask` parameter to include
   the event types you are interested in, then remount BeeGFS.

6. In the `/etc/beegfs/beegfs-watch.toml` file configure one or more subscribers
   following the inline directions. 

7. Reload the configuration by running `systemctl reload beegfs-watch`. 

8. Verify the new configuration was applied successfully by looking at the logs using `journalctl -u
   beegfs-watch`.

9.  If you want to uninstall/cleanup stop Watch with `systemctl stop beegfs-watch` then use the
   package manager to remove it. For example on Ubuntu run `sudo dpkg -r beegfs-watch` to uninstall
   the service and binaries or `sudo dpkg --purge beegfs-watch` to also remove configuration files.

## Using Container Images

While there are many ways to run containers, this is the most basic example
using `docker run` that can be adapted for container orchestrators like Docker
Compose or Kubernetes.

Prerequisites: 

* BeeGFS Metadata service logging events on the base OS at
  `sysFileEventLogTarget=/run/beegfs/eventlog`.
* A configuration file with the desired metadata configuration and one or more subscribers at
  `/etc/beegfs/beegfs-watch.toml`.
* A subscriber listening on the default Docker bridge network at
  `172.17.0.1:50052`.
* Docker installed on the same server as the Metadata service.
* The Watch container image downloaded from the [GitHub container
  registry](https://github.com/ThinkParQ/beegfs-go/pkgs/container/beegfs-watch)
  (`docker pull ghcr.io/thinkparq/beegfs-watch:latest`)or [built
  manually](build/README.md).
  * Currently this package is private, so you must first [login to the
    registry](https://docs.github.com/en/packages/working-with-a-github-packages-registry/working-with-the-container-registry#authenticating-to-the-container-registry).

We'll start with the full `docker run` command then we'll break it down
step-by-step:

```shell
docker run \
    -v /run/beegfs:/run/beegfs \
    -v /etc/beegfs/:/etc/beegfs \
    ghcr.io/thinkparq/beegfs-watch:latest \
    --cfg-file=/etc/beegfs/beegfs-watch.toml \
    --log.type=stdout
```

(1) The `-v /run/beegfs:/run/beegfs` bind mounts the /run/beegfs directory on the host machine's
filesystem into the container. This is what allows the containerized Watch service to access the
Unix socket where the metadata service expects to output events.

(2) The `-v /etc/beegfs:/etc/beegfs` bind mounts the configuration file with the metadata
configuration and subscriber list into the container.

(3) The `ghcr.io/thinkparq/beegfs-watch:latest` is the name of the container image. If you just want
to download the latest image from GitHub Container Registry this can be used as is. Otherwise
specify name of the container image built using `make package-all` (then see `docker images` if
you're not sure the name).

(4) The rest of the command are regular arguments passed to Watch and at minimum should specify the
path to the config file (inside the container) with the metadata and subscriber configuration
`--cfg-file=/etc/beegfs/beegfs-watch.toml`. Here the log type is also set to stdout using a flag.

> The default configuration file sets the log.type to "logfile", but typically containers are setup
to log to stdout which is why we override it here using a flag. Adjust as needed based on your
requirements.

Here we provided some Watch arguments as flags, but they can also be specified using environment
variables or a configuration file. For example we could have specified the `--log.type` as an
environment variable. Note the use of a double underscore between log and type which is replaced
with a period by the config parser:

 ```shell
docker run \
    -v /run/beegfs:/run/beegfs \
    -e Watch_LOG__TYPE=stdout \
    ghcr.io/thinkparq/beegfs-watch:latest --cfg-file=/etc/beegfs/beegfs-watch.toml
```

The ability to use a mix of flags, environment variables, and a configuration file provides
flexibility when running Watch in a container.

## For Developers

This section presumes you are not working with a prebuilt Watch binary, and
instead have cloned the repository locally and already have the following
prerequisites installed: 

* A compatible version of [Go](https://go.dev/doc/install).
  * Note if you want to build the project using the Makefile, you must have the
    same version of Go installed that is listed in the `GO_BUILD_VERSION`
    variable in `hack/check-go-version.sh`. Generally building the project with
    a newer version of Go should never cause problems, and this check is mostly
    to ensure when we build/release official packages using GitHub Actions we
    know exactly what version of Go was used.
* The [Go
  Licenses](https://github.com/google/go-licenses?tab=readme-ov-file#installation)
  tool must be installed and in your $PATH.
  * Note this is only required to build using the Makefile.

For Watch to start it requires:

* The path to the Unix socket where the BeeGFS metadata server will log file
  system modification events (specified in `/etc/beegfs/beegfs-meta.conf` as
  `sysFileEventLogTarget`). This file doesn't have to exist before Watch is
  started, and generally Watch should be started before the metadata service
  is actually sending events to the socket.
  * Note the parent directory of `sysFileEventLogTarget` must be accessible by
    the user executing Watch.
* A path to a configuration file that will be used to specify subscribers using `--cfg-file`. Note
  it is possible to start Watch and later add subscribers to the file reloading the configuration by
  sending Watch a hangup signal.

To configure and start Watch:

1. Copy the default config file from `build/beegfs-watch.toml` or create an empty TOML file: `touch
   scratch/beegfs-watch.toml` and define the metadata service you want to watch.
```toml
[[metadata]]
event-log-target = '/run/beegfs/eventlog'
event-buffer-size = 4194304
event-buffer-gc-frequency = 419430
```
1. Define what management service Watch should use for license verification. Note the management
   must already have a license installed that includes support for Watch.
```toml
[management]
address = "127.0.0.1:8010"
tls-cert-file = "/etc/beegfs/cert.pem"
tls-disable-verification = true
tls-disable = true
```
1. Start Watch: `go run cmd/beegfs-watch/main.go --cfg-file=scratch/beegfs-watch.toml`
   1. At this point Watch will begin buffering any events it receives from the metadata service
      until it reaches the `event-buffer-size`, then the oldest events will start to be dropped.
      This default behavior keeps as many historical events as possible so subscribers can be added
      after Watch has started.
2. Add a subscriber to the `beegfs-watch.toml` file. Note this subscriber doesn't
   actually have to exist if you just want to experiment with Watch (it will
   simply log unable to connect):
```toml
[[subscriber]]
id = 1
name = 'test-subscriber'
type = 'grpc'
grpc-address = 'localhost:50051'
grpc-disable-tls = true
```
1. From a new terminal send Watch hang up signal:
   1. Determine the Watch process ID by running `pgrep -a main` and ensuring
      the only PID returned is for Watch (the easiest way to tell is based on
      the flags as the executable will look like
      `/tmp/go-build2521339517/b001/exe/main`).
   2. Run `kill -HUP $(pgrep main)` or run `kill -HUP <PID>` with the
      appropriate process ID.
2. In the original terminal you should observe Watch log it is adding a
   subscriber and start attempting to connect. If the subscriber doesn't exist
   Watch will continue trying to reconnect with an exponential back off.

# Configuring Watch

## Overview

Watch supports multiple configuration sources, and more than one source can
be used at once. Except for the subscriber configuration, options can be
specified in multiple places, and the final configuration is determined based on
the following precedence order: 

1. Command line flags
2. Environment variables
3. TOML Configuration file
4. Built-in defaults

This provides flexibility in defining a base application configuration that can
be changed at runtime based on the environment (for example to set different log
destinations in production versus pre-production). For a list of all available
configuration options including their defaults see `--help`. 

### Specify Configuration Using Flags

Watch can be fully configured to run using command line flags. See `--help`
for all options.

### Specify Configuration Using Environment Variables 

All options listed in `--help` can be specified as environment variables, but
option names needs to be reformated to be valid/standard Linux variable names by
applying the following rules:

* All variables that apply to Watch must be prefixed with `Watch_`.
* All letters must be uppercase. 
* All dots (.) in the parameter name must be replaced with a double underscore (__).
* All hyphens (-) in the parameter name must be replaced with an underscore (_).

For example the flag `--log.type` would be specified as `Watch_LOG_TYPE`.

### Specify Configuration using a TOML Configuration File

When this option is used Watch should be started with the flag
`--cfg-file=<PATH>` where `<PATH>` is an absolute path to the configuration file
to use. Except for `--cfg-file` all options specified in `--help` can be
specified in a configuration file. Options are specified based on TOML
formatting rules:

* The list of subscribers is specified as individual subscribers using a TOML array
  (`[[subscriber]]`) with options for that subscriber specified beneath as `key=value`. For example: 

```toml
[[subscriber]]
id = 1
name = 'subscriber-1'
type = 'grpc'
grpc-address = 'localhost:50051'
[[subscriber]]
id = 2
name = 'subscriber-2'
type = 'grpc'
grpc-address = 'localhost:50052'
```

* For all other options, the   part of each option name (preceding the dot)
  indicates what internal component the configuration is directed at, and is
  specified once as a table header followed by the configuration options for
  that component as `key=value`. For example to specify the `--log.type`,
  `log.file`, and `log.level` in TOML:
```toml
[log]
type = 'logfile'
file = '/var/log/Watch/Watch.log'
level = 3
```

All values must be specified using the rules for TOML [key/value
pairs](https://toml.io/en/v1.0.0#keyvalue-pair), notably:

* All keys are specified as is and followed by an equals sign and their value.
* Values are specified based on the type they represent:
  * All strings are surrounded by single 'quotes'. 
  * All numbers are listed with no quotes.
  * All boolean (true/false) values are listed with no quotes. 

## Configuring Subscribers

IMPORTANT: At this time it is only possible to configure subscribers using a configuration file.
Configuring subscribers using environment variables or flags is not supported.

When configuring subscribers, the following options must be specified for each
subscriber regardless of type: 

* id
* name
* type

The `id` option is used to keep track of what events should/have been sent to a
particular subscriber. This means all other subscriber configuration (including
the type) can be changed without losing events intended for a particular
subscriber as long as the ID remains the same. For example if an event with
sequence ID 5 was sent and acknowledged by subscriber "1", then subscriber "1"
was disconnected and reconfigured with a new hostname (perhaps due to a server
migration), when we reconnect we'll continue sending events starting with
sequence ID 6 (presuming no events were dropped due to a buffer overflow). The
`name` option no special significance and is intended to help users identify
this subscriber in logs and configuration files. The `type` option is used to
specify what protocol should be used to communicate with this subscriber.
Additional options will be needed based on the provided type. 

### gRPC

To use the gRPC subscriber type specify `type: grpc` then use the following
options to configure the subscriber:

* grpcHostname (required): The IP address or hostname of the subscriber. No
  default.
* grpcPort (required): What port the subscriber is listening on. No default.
* grpcDisconnectTimeout (optional): When a local disconnect is requested, how
  long to wait (in seconds) for the remote subscriber to also shutdown their end
  of the connection before just closing the connection. If this value is to
  short then we may not receive acknowledgement of events the subscriber has
  processed (subscribers should resend the acknowledgement when they reconnect).
  If this value is to long, then reconfiguration attempts or shutting down
  Watch may hang for an inconvenient amount of time. Default: 30 (seconds). 

It is recommended to use TLS to authenticate subscribers and encrypt all
communication. If subscribers are configured to use a valid certificate signed
by a recognized Certificate Authority (CA), or your organization is using its
own signing server and adding the necessary certificate(s) to the system running
Watch, no additional steps will be required. If this is not the case there
are a few options: 

#### OPTION 1: Specify the path to a self-signed certificate

While self-signed certificates are not generally recommended outside development
or testing environments, they are a better option than disabling TLS if
obtaining a certificate signed by a recognized Certificate Authority is not
possible. Ensure the private key is appropriately secured as anyone with access
to the key can impersonate the subscriber.

(1) If needed generate a self-signed certificate (replace `localhost` with the
hostname of the system running the subscriber and update `-days 365` to reflect
how long the certificate should be valid):
```shell
openssl req -x509 -newkey rsa:4096 -keyout key.pem -out cert.pem -days 365 -nodes \
-subj "/CN=localhost" \
-addext "subjectAltName = DNS:localhost"
```
(2) Use this certificate file and key file when starting the gRPC subscriber.

(3) When configuring the subscriber in Watch, specify the path to the
self-signed certificate (`cert.pem` in this example) using
`grpc-tls-cert-file=<PATH>`.

NOTE: When the path to a self-signed certificate is specified it takes
precedence over all other TLS configuration. Any other local installed
certificates and the option to disable TLS will be ignored.

#### OPTION 2: Add subscriber certificate(s) to the system-wide store of trusted certificates on the Watch server

If the root certificate for the Certificate Authority used to sign your
certificate is not in the operating system's default list of trusted root
certificates or you are using a self-signed certificate, you can add your
certificate to the the system's list of trusted certificates. This allows you to
start the subscriber without specifying `grpc-tls-cert-file`. The exact
steps will vary based on your environment and Linux distribution, for example on
Ubuntu you would use the following commands (note the extension change from .pem
to .crt is intentional/required):

```shell
sudo cp cert.pem /usr/local/share/ca-certificates/cert.crt
sudo update-ca-certificates
```

#### OPTION 3: Disable TLS (not recommended)

This option means all gRPC messages including potentially sensitive information
about file paths/names inside the file system will be sent in clear text over
the network.

(1) When configuring the subscriber set `grpc-disable-tls=true`. 
 
## Updating Configuration (without restarting Watch)

Currently the following configuration can be updated after Watch has started
without requiring a restart:

* Subscribers
* Log level

The primary intent is to allow subscribers to be added, removed, and updated
without impacting other subscribers or dropping events. Allowing the log level
to be updated on-the-fly simplifies troubleshooting issues without requiring a
full restart, which may not be possible, or may loose state that makes issues
difficult to reproduce. 

Only configuration set using a configuration file can be updated without a
restart. Because flags and environment variables cannot be updated once the
application has started (and they have the highest precedence), their
configuration is immutable. To update the configuration first make the
appropriate changes to the configuration file then send the Watch process a
signal hang up (SIGUP). For example by running `kill -HUP <PID>` where PID is
the process ID from `pgrep`. If Watch was started using systemd then you can
run `systemctl reload beegfs-watch`.

# Shutting Down Watch 

The application is wired to shutdown on a SIGTERM or SIGINT. The first signal
attempts a clean shutdown by first disconnecting the metadata service but
continuing to accept configuration updates and sending outstanding events in the
buffer to all subscribers. If a second signal is received, the application will
attempt to immediately shutdown by disconnecting all subscribers and exiting,
thus dropping any outstanding unacknowledged events in the buffer. If the
metadata service is not also buffering events these events will be lost forever.

Currently the clean shutdown is setup so events must be sent to all subscribers
regardless if they were connected or not when the shutdown was initiated. This
means the first signal could hang forever if disconnected subscriber(s) never
reconnect to receive their outstanding events. If you want to be sure events
were sent to certain subscriber(s) you can update the subscriber configuration
while the application is shutting down, for example to correct misconfiguration
or remove a subscriber.

# Advanced

## Developing gRPC Subscribers

gRPC (gRPC Remote Procedural Calls) is an open source RPC framework developed by
Google. It uses Protocol Buffers as its Interface Definition Language (IDL) and
the protocol buffer definitions for Watch are maintained in the [protobuf](https://github.com/ThinkParQ/protobuf/tree/main)
as [`proto/Watch.proto`](https://github.com/ThinkParQ/protobuf/blob/main/proto/Watch.proto).
This file along with the Protocol Buffer compiler (protoc) and the gRPC plugin 
allows client and server code to be automatically generated in various programming 
languages including C++, Go, and Rust. Generated code for different languages is 
also maintained in the protobuf repository, for example for Go these files are under 
`go/` in files ending in `.pb.go`. See the [README](https://github.com/ThinkParQ/protobuf/tree/main?tab=readme-ov-file#beegfs-protocol-buffers-)
in the protobuf repository for how to get started in the language of your choice.

Watch uses the generated gRPC client code to connect and send messages (file
system modification events) to one or more subscribers that implement a gRPC
server that implements the interfaces defined by the Watch Protocol Buffers.
Note this section is not intended to replace the [Protocol
Buffers](https://protobuf.dev/overview/) and [gRPC
documentation](https://grpc.io/docs/what-is-grpc/introduction/), but rather
provide details on how gRPC is used in the context of Watch, and the expected
way subscribers are should consume this functionality to optimize performance
and avoid lost or duplicate events. A fully functional example is provided at
`cmd/test-subscriber/main.go` with the inline comments providing step-by-step
directions for getting started in Go. For those that prefer to learn by example
this may be an easier place to start. 

### Implementation Details
Watch implements a single `Subscriber`
[service](https://grpc.io/docs/what-is-grpc/core-concepts/#service-definition)
that provides a single `ReceiveEvents` [bidirectional streaming
RPC](https://grpc.io/docs/what-is-grpc/core-concepts/#bidirectional-streaming-rpc).
Using this RPC Watch (the client) sends one or more `Event`
[messages](https://protobuf.dev/programming-guides/proto3/) to gRPC servers
(subscribers) that implement the `Subscriber` service and `ReceiveEvents` RPC,
and receives back `Response` messages from the server. The `Event` messages
carry all the information from BeeGFS file system modification event
[messages](https://doc.beegfs.io/latest/advanced_topics/filesystem_modification_events.html#messages)
and the `Response` message carries the sequence ID of the latest event the
subscriber has processed.

We use a bidirectional streaming RPC for a few reasons: 

(1) Performance is greatly optimized over a unary RPC as we can reuse the same
underlying TCP connection to send multiple events and responses.

(2) Once established, bidirectional streams use independent streams, meaning
clients and servers can read and write messages in any order. This allows the
client to initiate a connection then wait for the server to send a message (for
example a response indicating the last completed event), then use that
information to determine what message it should send first (for example the
expected next event in the sequence for this subscriber).

When a gRPC subscriber is properly implemented, Watch provides the following
guarantees: 

* As long as buffer space is available on the Watch server, events will not
  be dropped due to a network issue or the subscriber disconnecting due to a
  planned/unplanned reboot.
* Events will only be sent once (no duplicate events).

### Best Practices 

To properly implement a gRPC subscriber and take advantage of the capabilities
provided by gRPC and bidirectional streams subscribers should adhere to the
following best practices. 

#### Avoid dropped events by acknowledging events after they're processed

We cannot solely rely on gRPC to avoid dropped events when the connection is
disrupted unexpectedly, especially if the disruption was due to a server-side
shutdown/panic. This is inevitable with any network protocol,  while the client
may receive acknowledgement once the message is received into the server's
network buffer, there is no way to guarantee the message made it to the
application and the application had a chance to do something meaningful with it
(like save it to stable storage) before the crash. To fully prevent this we must
provide a mechanism at the application level to acknowledge events.

To this end subscribers should use `stream.Send()` to send `Responses`
acknowledging the `completed_seq` of the last event they have processed. In this
context, processed mean the subscriber has handled the event, perhaps saving it
to a database or taking some action, and there is no chance it will need
Watch to resend the event. Because Watch uses a bidirectional stream,
generally subscribers are expected to use one thread to read new events and
another thread to send acknowledgements so events can be received/processed at a
different rate than they are acknowledged.

While retaining events for some period of time after they are sent to a subscriber is critical to
avoid dropped events, there is a finite number of events (as defined by the `event-buffer-size`
configuration) Watch will keep in its buffer before old events are dropped to make way for new
ones. Thus it is important once subscribers handle an event they acknowledge the event sequence ID
back to Watch so the corresponding buffers can be freed. Internally Watch performs garbage
collection periodically (determined by `event-buffer-gc-frequency`), freeing up in bulk multiple
events once they are acknowledged by all subscribers. If subscribers fail to send acknowledgement
(due to misconfiguration/implementation or being disconnected), once the Watch buffer is full, we
no longer do bulk garbage collection and fallback to deleting individual events as new events are
received from the Metadata service. While this minimizes the number of events dropped due to a
buffer overflow, it will incur a severe performance penalty, thus subscribers should not rely on
events eventually being dropped when the buffer overflows in lieu of properly acknowledging events.

TL;DR - Don't think sending acknowledgements is optional.

#### Optimize performance by not acknowledging every event

While eventually failing to send acknowledgements will impact performance, if
Watch required subscribers to send a response acknowledging each event we
would add quite a bit of overhead just sending back "control" messages. This
would especially impact performance if Watch required subscribers to
acknowledge an event before the next one is sent. 

To optimize performance Watch does not recommend acknowledging each event,
nor does it require acknowledging an event before the next event is sent.
Instead subscribers should acknowledge events on a fixed time based interval,
generally every second allows for reasonable performance while avoiding buffer
overflows. Note subscribers should not acknowledge events based on number of
events received, as there may be extended periods where no events are sent
causing events to be trapped in the buffer until enough new events are received
to trigger a response. Acknowledgements are expected to be sent in order,
meaning if sequences 1, 2, and 3 were sent and 3 is acknowledged, then 1 and 2
are implicitly acknowledged and Watch can remove all three events from its
buffers.

TL;DR - Don't acknowledge every event, keep a rolling counter and acknowledge
events every second.

#### Avoid duplicate events by acknowledging the last event received when reconnecting

Watch keeps track of the last event sent and the last event acknowledged for each subscriber. Events
are not removed from the buffer until they are acknowledged. When a subscriber disconnects it is
possible some of the events that were sent were not actually received/processed by the subscriber.
While we could simply start resending from the last acknowledged event, this means we could send the
same event multiple times, which some subscribers may not expect. To prevent this when a subscriber
connects/reconnects, Watch waits for the subscriber to acknowledge the last event received, which it
allows it to send the next event in the sequence. 

How long Watch wait for the last event to be acknowledged is based on the global
`--handler.max-wait-for-response-after-connect` or per subscriber `wait-for-response-after-connect`
configuration.Prior to BeeGFS 8.4 `max-wait-for-response-after-connect` defaulted 2 seconds before
sending events from the last acknowledged event. Starting in 8.4 this defaults to "0" which causes
Watch to wait indefinitely, which was determined to be safer default behavior. 

Subscribers that send acknowledgements can send an initial ack with a special sequence ID to
negotiate where to start the event stream:

* Sequence ID `0` will send all events from the earliest stored in Watch's buffers (as of 8.0).
* Sequence ID `18446744073709551615` (max uint64) will not send any historical events and start with
  the next event received from the metadata node (as of 8.4).

TL;DR - After reconnecting, immediately acknowledge the sequence ID of the last event you received.
