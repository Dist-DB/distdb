# Using the platform

# Server

For the default configuration, use 

```bash
cd ./server
'./debug.sh'
```


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

there is also a 'help' feature that will provide other commands. The service WILL BE 100% compatible with the MySQL8.0.x SQL dialect (in time)