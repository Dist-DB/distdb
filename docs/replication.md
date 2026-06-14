
# Replication

- An AFFINITY is a logical grouping of datanodes
- All datanodes have an optional AFFINITY identifier

# Affinty Definition

- Has an Identifier + Password (HASHED) - final key = hash!{identifier.lower:key}
- AFFINITY INDEX is a list of datanodes + addresses (ip address + port)


# Startup

- Do I have an Affinity Configuration (using the key above as the identifier)
- YES - Load Affinity
    - Try to connect to Datanode in Replication Topology
        - Success
            - Send request for List of Databases
        - Failure
            - Try next in list

- NO - Join Affinity
    - Success 
        - Fetch list of nodes in Affinity
        - Start replication of Affinity Databases + Tables + la la la 
    - Failure
        - Something is seriously wrong

- NO - Create new Affinity
    - Establish new Replication Document (using key above)
    - Stay silent as no nodes yet

### Request - Coming from another Datanode

- Datanode Wants to Join Affinity
    - Is Node in Affinity?
        - Send list of datanodes (in replication document)
    - Is New Node + Key Match ?
        - Success
            - Send list of datanodes (in replication document)

- Datanode Wants to Fetch List of Databases
    - Is Node in Affinity?
        - Success
            - Provide List of Databases..
        
- Datanode Wants to Enumerate Database Object
    - Is Node in Affinity?
        - Success
            - Provide List of DatabaseEntities (via Catalog) + TransactionIndex

- Datanode Wants DatabaseEntity (catalog.rs - line 39)
    - Is Node in Affinity?
        - Success
            - Provide DatabaseEntity WAL


