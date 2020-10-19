# iterateRelationship

## Purpose

Modern servers with many processor cores have considerable computing power, which can be exploited to speed up operations including ETL when appropriate parallel algorithms are used. In Neo4j, the `apoc.periodic.iterate()` procedure can be used to import nodes into a running database using parallel processing, provided that the source data is sorted such that no two threads are ever writing information to the same node at the same time. This can usually be achieved by sorting rows of data by the node key; as long as all rows with the same node key are handled by the same thread, there will be no conflicts over nodes.

Importing relationships into a running Neo4j instance using parallel processing is much more difficult. For an arbitrary list of data about relationships between nodes, there is no simple way to sort the data by node key to avoid conflicts between threads, as each relationship connects to two nodes. Consequently, a procedure like `apoc.periodic.iterate()` cannot be used in general to import relationships in parallel.

The procedure described here, `iterateRelationship()`, was developed to circumvent this problem. In this procedure, a list of data about relationships is sorted into bins based on the keys of the source and target nodes. The relationships in these bins are imported in a series of rounds; in each round, multiple threads import relationships from a set of bins such that no two threads are ever accessing the same node. In testing, `iterateRelationship()` imported relationships about five times as fast as a single-threaded import using `apoc.periodic.iterate()`.

## Use

`iterateRelationship()` is based on `apoc.periodic.iterate()` and is invoked using very similar syntax. The procedure is called with two Cypher statements: the first returns a set of records, and the second consumes the records produced by the first. In addition to the data that the second statement will consume, the first statement must return two special variables that identify the source and target nodes; these are used by `iterateRelationship()` to sort relationships into bins for parallel processing.

The following is a simple example of `iterateRelationship()` usage:

```
CALL iterateRelationship('
    CALL apoc.load.jdbc("jdbc:connection_info_here", "
        select personID, phoneNum from database_table
    ") YIELD row
    RETURN row, toInteger(row.personID) as sourceID, toInteger(row.phoneNum) as targetID
','
    MATCH (p:PERSON {id: row.personID})
    MERGE (ph:PHONE {phone: row.phoneNum})
    MERGE (p)-[:HAS_PHONE]->(ph)
','sourceID','targetID', {batchSize:1000, iterateList:true, retries:5, concurrency: 32});
```

This code runs as follows:
- The first Cypher statement connects to a database and runs a SQL query, yielding a set of records with the variable `row`.
- The first statement returns `row` as well as the variables `sourceID` and `targetID`, which are derived from subfields of `row`. `sourceID` is the field `row.personID`, which identifies the PERSON node. `targetID` is the field `row.phoneNum`, which identifies the PHONE node.
- The second statement takes the information in `row` and uses it to create HAS_PHONE relationships between PERSON and PHONE nodes.
- After the second statement come the strings `'sourceID'` and `'targetID'`, which must match the extra variables returned from the first statement that identify the source (PERSON) and target (PHONE) nodes.
- After that comes a map of parameters; see the [documentation for `apoc.periodic.iterate()`](https://neo4j.com/docs/labs/apoc/current/graph-updates/periodic-execution/#commit-batching) to understand the function of these parameters.

The `sourceID` and `targetID` variables are important, as these determine how relationships are sorted into bins for parallel processing. For best performance, these variables should be integers, but other data types will work too. The values of `sourceID` and `targetID` must never be null, as that will cause the procedure to throw a NullPointerException.

While the available parameters are mostly the same as those for `apoc.periodic.iterate()`, a few deserve additional explanation:
- The parameter `parallel` is not used in `iterateRelationship()` - all calls to `iterateRelationship()` use parallel processing.
- The parameter `concurrency` must be a power of 2. If not, it will be rounded down to the nearest power of 2.
- `iterateRelationship()` has an additional parameter `groupSameSourceTarget`. When this is set to `true`, the procedure will ensure that records with the same values for `sourceID` and `targetID` are included in the same batch. For this to work, the parameter `iterateList` must be set to `true` (to make the second statement process batches of records from the first statement as a single list), and the results from the first statement must be sorted such that records with the same values for `sourceID` and `targetID` always appear consecutively. This is useful when the second statement performs grouping on records with the same `sourceID` and `targetID`. When `groupSameSourceTarget` is `true`, the actual batch sizes may differ slightly from the configured `batchSize` parameter.

## Interpreting log messages

`iterateRelationship()` generates constant output in `logs/neo4j.log`, which can be helpful in understanding what's happening:
- Records are imported in cycles. When the concurrency is 2<sup>n</sup>, each cycle will consist of 2<sup>n+2</sup> rounds. For example, when the concurrency is 32, each cycle will have 128 rounds.
- Each cycle operates on a BinSet, which must be loaded before the cycle can begin. The BinSet is filled with records that have been returned by the first Cypher statement and will be consumed by the second Cypher statement.
- When a process takes more than one cycle to run, the BinSet for a given cycle is loaded while the previous cycle is running. Thus, if the message "Loaded new BinSet" is printed during the middle of a cycle, the second Cypher statement in `iterateRelationship()` is the rate-limiting step. If the statement "Loaded new BinSet" is printed at the end of a cycle - perhaps with some wait time between when the cycle ends and when the BinSet is loaded - the first Cypher statement is the rate-limiting step.
- Every time a BinSet is loaded, an entropy calculation is printed in the log file. A higher number for entropy means a more uniform distribution of relationships across bins, which leads to a more efficient data import.  When the concurrency is 2<sup>n</sup>, the maximum possible entropy is 2n+2 bits. For example, when the concurrency is 32, the maximum possible entropy is 12 bits. To maximize the entropy and therefore the import efficiency, choose source IDs and target IDs that are as different as possible between the relationships being imported, as these determine how the relationships are sorted into bins.
- For each round, a message is printed in the log file, along with the amount of free heap memory. These messages are helpful in tracking how fast the process is moving as well as whether it is running out of memory.
- Occasionally the procedure prints information about deadlocks. `iterateRelationship()` is designed to avoid deadlocks; when deadlocks occur, they are due to the implementation of the Neo4j lock manager and not to the `iterateRelationship()` code. When a deadlock occurs, the default behavior is to rerun the transaction that failed because of the deadlock. If the same transaction fails 5 times (by default) it will be discarded. **Thus, as long as no transaction gets to "Deadlock on try 4", no data will be lost.**

## Maintenance

`iterateRelationship()` depends on the APOC library and has been designed to work with APOC v3.5.0.5. Upgrades to APOC may require revisions to the `iterateRelationship()` code. As `iterateRelationship()` is based on `apoc.periodic.iterate()`, the file [`Periodic.java`](https://github.com/neo4j-contrib/neo4j-apoc-procedures/blob/3.5/src/main/java/apoc/periodic/Periodic.java) in the APOC library should be the first place to look to determine what, if any, revisions are needed to the `iterateRelationship()` code.
