CREATE INDEX ON :NodeType(node_id);


// Import nodes
CALL apoc.periodic.iterate('
	CALL apoc.load.csv("file:///erNodes.csv", {}) yield map
	RETURN toInteger(map.nodeID) AS nodeID
','
	CREATE (n:NodeType {node_id: nodeID})
',{batchSize:10000, parallel:true, concurrency:32});


// Import relationships
CALL apoc.periodic.iterateRelationship('
	CALL apoc.load.csv("file:///erRels.csv", {}) yield map 
	RETURN toInteger(map.sourceID) AS sourceID, toInteger(map.targetID) AS targetID
','
	MATCH (n:NodeType {node_id: sourceID}), (m:NodeType {node_id: targetID})
	CREATE (n)-[r:RelType]->(m)
	SET r.prop="test"
','sourceID','targetID', {batchSize:1000, iterateList:true, concurrency:32});


// Check counts to make sure all relationships are in
MATCH ()-[r:RelType]->() RETURN count(r);
