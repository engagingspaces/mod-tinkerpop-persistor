# Vert.x Tinkerpop Persistor Module

This is a simple database persistence module for graph databases that uses [Tinkerpop Blueprints](https://github.com/tinkerpop/blueprints) and [Tinkerpop Gremlin](https://github.com/tinkerpop/gremlin) to communicate to the underlying graphdb. 
The purpose of Tinkerpop is to abstract away vendor-specific code when connecting to the database. Thus it's purpose is similar to JDBC, but then specifically targeted to graph databases. The following is taken from the [Blueprints home on Github](https://github.com/tinkerpop/blueprints/wiki):

> Blueprints is a collection of interfaces, implementations, ouplementations, and test suites for the property graph data model. Blueprints is analogous to the JDBC, but for [graph databases](http://en.wikipedia.org/wiki/Graph_database). As such, it provides a common set of interfaces to allow developers to plug-and-play their graph database backend. Moreover, software written atop Blueprints works over all Blueprints-enabled graph databases.

Therefore the code in this module has been tested against two separate graph database products, the well-known [Neo4J](http://www.neo4j.org) and less well-known [OrientDB](http://www.orientdb.org). See the Maven POM file for version details.

The module is used in a similar way to the [Mongo Persistor Module](https://github.com/vert-x/mod-mongo-persistor) where on each message that is send the 'action' key determines the functionality to invoke.

> NOTE: This module is created for the purpose of testing and experimentation with both Vert.x and graph databases, and is by no means production-ready!

Module Configuration
--------------------

The configuration of the Vert.x module depends on the graph database implementation that is used. The configuration can be specified in the `mod.json' file. In all cases it must be included in a `tinkerpopConfig` object.

For Neo4J a sample configuration looks like this:

```
{
    "address" : "tinkerpop.persistor",
    "tinkerpopConfig" : {
        "blueprints.graph" : "com.tinkerpop.blueprints.impls.neo4j.Neo4jGraph",
        "blueprints.neo4j.directory" : "put your neo4j directory location here"
    }
}
```

And for OrientDB it looks like this:

```
{
    "address" : "tinkerpop.persistor",
    "tinkerpopConfig" : {
        "blueprints.graph" : "com.tinkerpop.blueprints.impls.orient.OrientGraph",
        "blueprints.orientdb.url" : "put your database url here",
        "blueprints.orientdb.username" : "yourUser",
        "blueprints.orientdb.password" : "yourPass",
        "blueprints.orientdb.saveOriginalIds" : true,
        "blueprints.orientdb.lightweightEdges" : true
    }
}
```

The [Blueprints wiki](https://github.com/tinkerpop/blueprints/wiki) has more information on the available configuration options for each supported database product.
Additional (sometimes more up-to-date) information can be found on the vendor's website. For OrientDB the latest info can be found here at [Configure the Graph](https://github.com/orientechnologies/orientdb/wiki/Graph-Database-Tinkerpop#configure-the-graph).

Supported actions
-----------------

The table below lists the actions that are currently supported by the module. Each action creates its own [Graph](https://github.com/tinkerpop/blueprints/blob/master/blueprints-core/src/main/java/com/tinkerpop/blueprints/Graph.java) to the underlying database to perform the operation. For both Neo4J and OrientDB these graphs are both transactional and have support for key indices.

> NOTE: Instead of `Vertex` and `Edge` the terminology of `Node` and `Relationship` can be used interchangeably in actions if you are used to that, or find that it clashes with `Vert.x` :-)

| Action                         | Description |
|--------------------------------|-------------|
| addGraph                       | Load a complete [Graph](https://github.com/tinkerpop/blueprints/blob/master/blueprints-core/src/main/java/com/tinkerpop/blueprints/Graph.java) (provided in [GraphSON format](https://github.com/tinkerpop/blueprints/wiki/GraphSON-Reader-and-Writer-Library)) in a single operation |
| addVertex, addNode             | Add a single Vertex to the graph |
| query                          | Perform a Gremlin query and return the resulting Vertices or Edges |
| getVertices, getNodes          | Get all Vertices from the graph or a filtered list by key / value |
| getVertex, getNode             | Get a single Vertex from the graph |
| removeVertex, removeNode       | Remove a single Vertex from the graph |
| addEdge, addRelationship       | Add a single Edge to the graph that connects two Vertices |
| getEdges, getRelationships     | Get all Edges from the graph or a filtered list by key / value |
| getEdge, getRelationship       | Get a single Edge from the graph |
| removeEdge, removeRelationship | Remove a single Edge from the graph |
| createKeyIndex                 | Create a new Key Index for the provided key on either Vertices or Edges |
| dropKeyIndex                   | Drop an existing Key Index on either Vertices or Edges |
| getIndexedKeys                 | Get the list of all existing Key Indices on either Vertices or Edges |
| flushQueryCache                | Remove one or all cached Gremlin queries |

Action Description
------------------

### General remarks

* Though the database may support transactions, it is not possible for a transaction to span multiple actions. Transactions are committed during or at the end of the action execution.
* Some database products ignore Id's supplied by the user in a graph, single Vertex or Edge. In those cases the `ignoresSuppliedIds` feature of the Graph is `true' and Id's are generated by the database (on transaction commit).
* Id's are defined as `Object` because the datatype depends on the database vendor. For example Neo4J uses `Integer`, while OrientDB uses `String` (e.g. `"#9:10"`).

### addGraph

Load a complete [Graph](https://github.com/tinkerpop/blueprints/blob/master/blueprints-core/src/main/java/com/tinkerpop/blueprints/Graph.java) (provided in [GraphSON format](https://github.com/tinkerpop/blueprints/wiki/GraphSON-Reader-and-Writer-Library)) in a single operation.

Vert.x message:

```
{
    "action": "addGraph",
    "graph":
    {
        "mode":"NORMAL",
        "vertices":
        [
            {"name":"marko","age":29,"_id":1,"_type":"vertex"},
            {"name":"vadas","age":27,"_id":2,"_type":"vertex"},
            {"name":"lop","lang":"java","_id":3,"_type":"vertex"},
            {"name":"josh","age":32,"_id":4,"_type":"vertex"},
            {"name":"ripple","lang":"java","_id":5,"_type":"vertex"},
            {"name":"peter","age":35,"_id":6,"_type":"vertex"}
        ],
        "edges":
        [
            {"weight":0.5,"_id":7,"_type":"edge","_outV":1,"_inV":2,"_label":"knows"},
            {"weight":1.0,"_id":8,"_type":"edge","_outV":1,"_inV":4,"_label":"knows"},
            {"weight":0.4,"_id":9,"_type":"edge","_outV":1,"_inV":3,"_label":"created"},
            {"weight":1.0,"_id":10,"_type":"edge","_outV":4,"_inV":5,"_label":"created"},
            {"weight":0.4,"_id":11,"_type":"edge","_outV":4,"_inV":3,"_label":"created"},
            {"weight":0.2,"_id":12,"_type":"edge","_outV":6,"_inV":3,"_label":"created"}
        ]
    }
}
```

Vert.x reply depends on the `ignoresSuppliedIds` feature of the Graph. If it is `false` then a simple `ok` status is returned, if `true' the reply contains the full graph as it was loaded, with the correct, database-generated Id's.

```
{
    "mode": "NORMAL",
    "vertices":
    [
        {"name":"marko","age":29,"_id":"#9:10","_type":"vertex"}
        //... etcetera
    ],
    "edges":
    [
        //...etcetera
    ],
    "status": "ok"
}
```

### addVertex, addNode

Add a single Vertex to the graph. If the message contains more than one Vertex, then only the first one is actually created while the rest is ignored.

Vert.x message:

```
{
    "action": "addVertex",
    "vertices": [
        {
            "project": "vert.x",
            "organization": "eclipse",
            "_id": "2",
            "_type": "vertex"
        },
        {
            "project": "mod-tinkerpop-persistor",
            "organization": "github",
            "_id": "3",
            "_type": "vertex"
        }
    ]
}
```

Vert.x reply:

```
{
    "_id": "#6:43",
    "status": "ok"
}
```

### query

Perform a Gremlin query and return the resulting Vertices or Edges. 
The current functionality allows the query to be passed as a string that is then compiled into a [Pipe](https://github.com/tinkerpop/pipes/blob/master/src/main/java/com/tinkerpop/pipes/Pipe.java) and (optionally) cached. Besides the query a starting Vertex or Edge must be specified by its `_id`.
By default the queries are cached in a [ConcurrentHashMap](http://docs.oracle.com/javase/7/docs/api/java/util/concurrent/ConcurrentHashMap.html) unless the `cache` key is `false`. A cached query can be flushed by providing the complete query string to the `flushCachedQueries` action.

Vert.x message:

```
{
    "action": "query",
    "starts": "Vertex",
    "cache": true
    "_id": "#9:10",
    "query": "_().in('HAS_CHILD_CONTENT').loop(1){it.loops < 3}{it.object.name == 'Root folder'}.path"
}
```

The Vert.x reply currently flattens the query results in a single JsonArray (which is probably not correct, but is enough for experimental use):

```
{
    "results" : 
    [
        {
            "name" : "User1 Home",
            "origId" : 11,
            "_id" : "#9:10",
            "_type" : "vertex"
        }, 
        {
            "name" : "Home",
             "origId" : 9,
            "_id" : "#9:8",
            "_type" : "vertex"
        }, 
        {
            "name" : "Root folder",
            "origId" : 8,
            "_id" : "#9:7",
            "_type" : "vertex"
        }
    ],
    "status" : "ok"
}
```

### getVertices, getNodes

Get all Vertices from the graph or a filtered list by key / value.

Vert.x message:

```
{
    "action": "getVertices",
    "key": "name",
    "value": "Root folder"
}
```

The Vert.x reply is in GraphSON format and includes the GraphSON `mode` (which defaults to `NORMAL`):

```
{
    "mode": "NORMAL",
    "vertices":
    [
        {
            "name" : "Root folder",
            "origId" : 8,
            "_id" : "#9:7",
            "_type" : "vertex"
        }
    ],
    "status": "ok"
}
```

### getVertex, getNode

Get a single Vertex from the graph.

Vert.x message:

```
{
    "action": "getVertex",
    "_id": "#9:10"
}
```

The Vert.x reply is in GraphSON format:

```
{
    "mode": "NORMAL",
    "vertices":
    [
        {
            "name" : "User1 Home",
            "origId" : 11,
            "_id" : "#9:10",
            "_type" : "vertex"
        }
    ],
    "status": "ok"
}
```

### removeVertex, removeNode

Remove a single Vertex from the graph.

Vert.x message:

```
{
    "action": "removeVertex",
    "_id": "#9:10"
}
```

Vert.x reply:

```
{
    "_id": "#9:10",
    "status": "ok"
}
```

### addEdge, addRelationship

Add a single Edge to the graph that connects two Vertices. The `_inV`, `_outV` and `_label` keys are required.

Vert.x message:

```
{
    "action": "addEdge",
    "edges":
    [
        {"_inV": 11, "_outV": 14, "flags": "+RW", "_label": "SECURITY"}
    ]
}
```

Vert.x reply:

```
{
    "_id": 17,
    "status": "ok"
}
```

### getEdges, getRelationships

This is similar to `getVertices` action.

### getEdge, getRelationship

This is similar to `getVertex` action.

### removeEdge, removeRelationship

This is similar to `removeVertex` action.

### createKeyIndex

Create a new Key Index for the provided key on either Vertices or Edges. The `elementClass` key must have a value of either `Vertex` or `Edge`. The `parameters` object can be omitted. Parameters are database vendor specific.

Vert.x message:

```
{
    "action": "createKeyIndex",
    "key": "name",
    "elementClass": "Vertex",
    "parameters":
    {
        // Optional. Look in database vendor documentation for supported parameters.
    }
}
```

Vert.x reply:

```
{
    "status": "ok"
}
```

### dropKeyIndex

Drop an existing Key Index on either Vertices or Edges.

Vert.x message:

```
{
    "action": "dropKeyIndex",
    "elementClass": "Edge",
    "key": "flags"
}
```

Vert.x reply:

```
{
    "status": "ok"
}
```

### getIndexedKeys

Get the list of all existing Key Indices on either Vertices or Edges.

Vert.x message:

```
{
    "action": "getIndexedKeys",
    "elementClass": "Vertex"
}
```

Vert.x reply:

```
{
    "keys": ["name", "_id"]
    "status": "ok"
}
```

### flushCachedQueries

Remove one or all cached Gremlin queries. Queries are cached with the full query string (and when `cache` is `true` on the `query` action on first-time use). If the `query` key is omitted, then the full cache is cleared.

Vert.x message:

```
{
    "action": "flushCachedQueries",
    "query": "_().in('HAS_CHILD_CONTENT').loop(1){it.loops < 3}{it.object.name == 'Root folder'}.path"
}
```

Vert.x reply:

```
{
    "status": "ok"
}
```