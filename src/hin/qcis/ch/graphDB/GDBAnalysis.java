package hin.qcis.ch.graphDB;

import hin.qcis.ch.graphDB.GraphDBMgt_neo4j.RelTypes;

import java.util.HashMap;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.traversal.Evaluators;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.graphdb.traversal.Traverser;

import CrawlJSON2GraphDB.Main;

public class GDBAnalysis {

	
	
	/**
	 * Get all your friends within n-degree.
	 * @param graphDb
	 * @param person
	 * @return
	 */
	private static Traverser getFriends(GraphDatabaseService graphDb, final Node person)
	{
	    TraversalDescription td = graphDb.traversalDescription()
	            .breadthFirst()
	            .relationships( RelTypes.KNOWS )
	            .evaluator( Evaluators.excludeStartPosition() );
	    return td.traverse( person );
	}

	public static JSONObject toJsonNode (Node graphNode, HashMap<Long, Integer> idxMapping)
	{
	    JSONObject JsonNode = new JSONObject();
	    JsonNode.put("ID", graphNode.getId());
	    for(String property : graphNode.getPropertyKeys())
	    {
	    	JsonNode.put(property, graphNode.getProperty(property));
	    }

	    JsonNode.put("name", graphNode.getProperty("UserName"));
	    JsonNode.put("group", Main.rdm.nextInt(10));
	    
	    System.out.println(graphNode.getId());
	    idxMapping.put(graphNode.getId(), idxMapping.size());
	    return JsonNode;
	}
	public static JSONObject toJsonLink (Relationship graphRelationship, HashMap<Long, Integer> idxMapping)
	{
	    JSONObject JsonLink = new JSONObject();
	    JsonLink.put("ID", graphRelationship.getId());

	    System.out.println("start node ID " + graphRelationship.getStartNode().getId());
	    long srcID = idxMapping.get(graphRelationship.getStartNode().getId());
	    JsonLink.put("source", srcID);

	    System.out.println("end node ID" + graphRelationship.getEndNode().getId());
	    long tgtID = idxMapping.get(graphRelationship.getEndNode().getId());
	    JsonLink.put("target", tgtID);
	    
	    JsonLink.put("value", Main.rdm.nextInt(10));
	    
	    for(String property : graphRelationship.getPropertyKeys())
	    {
	    	JsonLink.put(property, graphRelationship.getProperty(property));
	    }
	    
	    return JsonLink;
	}
	
	public static int outputTravser2Visual(GraphDatabaseService graphDb, Node node, int maxdegree)
	{
		HashMap<Long, Integer> idxMapping = new HashMap<Long, Integer>(); // 1 - index in GraphDB, 2 - index in visual
	    JSONObject jo = new JSONObject();

	    JSONArray jsNodes = new JSONArray();

	    JSONObject self = toJsonNode(node, idxMapping);
	    jsNodes.add(self);
	    
	    JSONArray jsLinks = new JSONArray();
	    
		int numberOfFriends = 0;
		String output = node.getProperty( "UserName" ) + "'s friends:\n";
		Traverser friendsTraverser = getFriends(graphDb, node );
		for ( Path friendPath : friendsTraverser )
		{
			if (friendPath.length() > maxdegree)
			{
				continue;
			}
			for(Node pathNode : friendPath.nodes())
			{
				if (idxMapping.get(pathNode.getId()) == null)
				{
					JSONObject jsPathNode = toJsonNode(pathNode, idxMapping);
					jsNodes.add(jsPathNode);
				}
			}
			for(Relationship pathLink : friendPath.relationships())
			{
				JSONObject jsLink = toJsonLink(pathLink, idxMapping);
				jsLinks.add(jsLink);
			}
		}
		
		jo.put("nodes", jsNodes);
		jo.put("links", jsLinks);
		
		System.out.println(jo.toJSONString().replace("/", ""));
		
		return numberOfFriends;
	}
	
	

	

}
