# Using the platform

The platform comprises of a number of elements

## Common

A set of commonly used functions and statics that are used throughout the project

## ServerLib

The core container for the service stack (as a Cargo Library) - This is used by the connector and also the server components

## Server

For the default configuration, use 

```bash
cd ./server
'./debug.sh'
```

The server will run in debug mode presenting all output to the console (using log) - This can be supressed by running the service in release mode (cargo run --release) as needed. Since this project is not production ready, i recommend using the debug version at the moment.

You can provide bootstrap peers for Kademlia discovery at startup:

```bash
cd ./server
cargo run datadir=./data servers=127.0.0.1:9400,10.0.0.5:9400
```

Bootstrap peer entries accept either `host:port` or multiaddr values such as `/ip4/127.0.0.1/tcp/9400`.

## Client

An example application featuring a range of features available to the platform - this is (at the moment) behind the core application development cycle - interested parties should look at the console application first

## Console

Using the same function to run server, 

```bash
cd ./console
'./debug.sh'
```

## Multi-Server Testing

On Server 1

```bash
RUST_LOG="info,connector::p2p=debug,serverlib::p2p=debug,console=debug" RUST_BACKTRACE=1 cargo run datadir=./data listen_addr=0.0.0.0 port=4001 node_id=sam01
```

On Server 2

```bash
RUST_LOG="info,connector::p2p=debug,serverlib::p2p=debug,console=debug" RUST_BACKTRACE=1 cargo run datadir=./data listen_addr=0.0.0.0 port=4002 node_id=sam02 servers=127.0.0.1:4001
```

Note that server 2 points to server 1 using the 'servers' directive

Then to connect to the cluster

```bash
RUST_LOG="info,serverlib::p2p=debug,console=debug" RUST_BACKTRACE=1 cargo run servers=127.0.0.1:4001
```


## Connecting the Console

You can also provide bootstrap peer candidates directly when launching console:

```bash
cd ./console
cargo run 127.0.0.1:9400 servers=127.0.0.1:4001
```

You should specify the server address that you wish to connect to - This will discover other datanodes in the p2p network

## Testing Console Functionality

When the console loads, use the following directives (if this is the first time)

```bash
connect root@server-node-01;
password password;
create database main;
show databases;
use main;
disconnect;
```

If the table is already created

```bash
connect root@server-node-01;
password password;
show databases;
use main;
show tables;
disconnect;
```



There is also a 'help' feature that will provide other commands. The service WILL BE 100% compatible with the MySQL8.0.x SQL dialect (in time).

The console will present information relating to the connectivity between client and server.

## Current Isolation Contract

The current explicit transaction behavior is equivalent to a read-committed style contract for staged DML:

- `insert`, `update`, and `delete` statements are staged per session while a transaction is active.
- Staged writes are not visible to other sessions before `commit`.
- On `commit`, staged writes are applied as a single grouped publish.
- On `rollback`, staged writes are discarded.

Within an explicit transaction:

- `select` statements execute against the transaction snapshot plus that session's staged writes.
- schema and other non-DML statement types are still rejected.

## Next Isolation Milestone

The next target is fuller snapshot isolation behavior:

- Reads inside one transaction are repeatable against its snapshot.
- A transaction sees its own staged writes.
- Concurrent write-write conflicts on the same logical row are rejected at commit for the later committer.

Write-write conflict behavior and repeatable-read behavior are both covered by active server tests.

Write-skew prevention for predicate-based invariants is now covered by an active server test.
Range/phantom conflict handling is the next serializable gap to close.