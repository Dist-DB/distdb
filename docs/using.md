# Using the platform

# Server

For the default configuration, use 

```bash
cd ./server
'./debug.sh'
```

The server will run in debug mode presenting all output to the console (using log) - This can be supressed by running the service in release mode (cargo run --release) as needed. Since this project is not production ready, i recommend using the debug version at the moment.

# Console

Using the same function to run server, 

```bash
cd ./console
'./debug.sh'
```

When the console loads, use the following directives

```bash
connect server-node-01;
create database main;
use main;
show databases;
disconnect;
```

There is also a 'help' feature that will provide other commands. The service WILL BE 100% compatible with the MySQL8.0.x SQL dialect (in time).

The console will present information relating to the connectivity between client and server.