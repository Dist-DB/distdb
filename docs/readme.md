
# A Distributed Database

This project is orientated to the development of a P2P distributed database

## Principles

The database platform conforms to the following principles

- The platform is architected to be a number (1 or more) server nodes 
- A servernode has a distinct identifier - this must be expressed at startup together with a data directory
- All nodes are interconnected over a p2p network using a common swarm/version identifier
- The p2p network uses Kadema for IP discovery for remote nodes
- A database may reside on any of the server nodes (one or more)
- Each database instance coordinates transactions with other database replicas
- A database follows a versioned SQL compatibility target based on MySQL 8.0.x for the supported statement set
- Each database maintains a transactional log of all data changes
- Security is defined on a node & database instance
- A standard set of SQL directives are supported by the service
- p2p clients may receive notifications on data changes pub/sub on a table/database level

- Changes to data are replicated to connected instances of a datanode sharing the same database
- Servers are interconnected using the pub/sub notification pump
- Subscriptions are managed using unique identifers using a hash!{databaseid:tableid}
- When a database modification occurs at a datanode, this is informed to the other datanodes that maybe connected via a publication
- A publication consists of 4 pieces of information - timestamp, serviceid, transactionid, eventtype
- All commited transactions are placed inside the transaction log - a notification of update is then published - then connected clients request transactions since the last transaction identifier
- Deletions are not considered in the transaction log as entry removals
- All content is stored in binary format and deserialized by the client
- Schema changes to tables are permitted and informed accordingly - A schema change is also a record in the transactional log
- Security structures are maintained in the transactional log
- When a schema change is noted, the data entries should conform to the schema format there after
- All transactions are recorded with timestamp (epochabs), userid, transaction-type and then data

- user-identifiers are hash!{username.lowercase}
- password-keys are hash!{databaseid:username.lowercase:password}

- tables comprise of fields {seqno:fieldname:fieldtype:size:nullability:indexed:default}
- fieldtypes may be int, uint, float, string[fixedsize], text, enum, spatial (long,lat,elevation), blob
- fieldsizes for int, uint and float are 8,16,32,64
- nullability for int, uint and float default to 0
- spatial default is {long: 0.0, lat: 0.0, ele: 0.0}

- tables have indexes based upon the schema definition
- all tables are subject to crud directives (create, retreive, update, delete)

## SQL Compatibility Target

The platform should treat MySQL 8.0.x as the compatibility baseline for SQL parsing, statement planning, and builder output.

The implementation should:

- parse the supported statement set using MySQL 8.0.x-compatible syntax rules
- translate parsed statements into one or more execution actions
- reject unsupported MySQL 8.0.x syntax explicitly rather than silently normalizing it


