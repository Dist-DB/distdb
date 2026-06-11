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

## Client

An example application featuring a range of features available to the platform - this is (at the moment) behind the core application development cycle - interested parties should look at the console application first

## Console

Using the same function to run server, 

```bash
cd ./console
'./debug.sh'
```

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