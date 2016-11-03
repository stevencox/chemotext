import org.neo4j.spark._

val neo = Neo4j(sc)

import org.graphframes._

// Need to open port 7687 on 'chemotext.mml.unc.edu'
val graphFrame = neo.cypher ("MATCH (n:Art) RETURN n").partitions (3).rows (100).loadGraphFrame
