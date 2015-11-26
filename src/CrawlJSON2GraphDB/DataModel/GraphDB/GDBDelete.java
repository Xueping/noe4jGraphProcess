package CrawlJSON2GraphDB.DataModel.GraphDB;

import java.util.HashMap;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.tooling.GlobalGraphOperations;

public class GDBDelete {

	public static void cleanDB(GraphDatabaseService graphDb)
	{

		for ( Relationship rel :  GlobalGraphOperations.at(graphDb).getAllRelationships() )
		{
			rel.delete();
		}
		for ( Node node :  GlobalGraphOperations.at(graphDb).getAllNodes() )
		{
			node.delete();
		}
	}
	
	public static void delete(GraphDatabaseService graphDb, Label label, HashMap<String, String> properties)
	{
		for ( Node node :  GlobalGraphOperations.at(graphDb).getAllNodes() )
		{
			if (GDBQuery.matchNode(node, label, properties))
			{
				for(Relationship link : node.getRelationships())
				{
					link.delete();
				}
				node.delete();
			}
		}
		
	}
	
	public static void delete(GraphDatabaseService graphDb, Relationship delLink)
	{
		for ( Relationship link :  GlobalGraphOperations.at(graphDb).getAllRelationships() )
		{
			if (GDBQuery.matchLink(delLink, link))
			{
				link.delete();
			}
		}
	}
	

}
