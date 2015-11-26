package CrawlJSON2GraphDB.DataModel.GraphDB;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.traversal.Evaluators;
import org.neo4j.graphdb.traversal.Traverser;
import org.neo4j.tooling.GlobalGraphOperations;


public class GraphDBDriver_neo4j implements IGraphDBDriver{

	public Label personLabel = DynamicLabel.label( "Person" );
	public static Random rdm = new Random();
	
	@Override
	public String queryEgoNetwork(GraphDatabaseService graphDBSvc,
			String propName, String propValue) 
	{
		int MAX_DEGREE = 2;
		return queryEgoNetwork(graphDBSvc, propName, propValue, MAX_DEGREE);
	}

	@Override
	public String queryEgoNetwork(GraphDatabaseService graphDBSvc,
			String propName, String propValue, int MAX_DEGREE) 
	{
		StringBuilder sb = new StringBuilder();
		try ( Transaction tx = graphDBSvc.beginTx() )
		{  
			for(Node node : graphDBSvc.findNodesByLabelAndProperty(personLabel, propName, propValue))
			{
				sb.append(travser2Json(graphDBSvc, node, MAX_DEGREE));
			}
			tx.success();
		}
		return sb.toString();
	}

	@Override
	public String queryUsers(GraphDatabaseService graphDBSvc, String propName, String propValue) 
	{	
		StringBuilder sb = new StringBuilder();
		sb.append("{\n");
		sb.append("\"nodes\":[\n");

		try ( Transaction tx = graphDBSvc.beginTx() )
		{  
			for ( Node node :  GlobalGraphOperations.at(graphDBSvc).getAllNodes() )
			{
				String nodeValue = node.getProperty(propName).toString();
				if (nodeValue.contains(propValue))
				{
					sb.append("{");
					sb.append(graphNode2String(node));
					sb.append("},\n");
				}
			}
			tx.success();
		}
		sb.delete(sb.length() - 1, sb.length());
		sb.append("\n],");
		sb.append("\n\"links\":[");
		sb.append("\n]");
		sb.append("\n}");
		return sb.toString().replace("/", "\\");
	}

	@Override
	public GraphDatabaseService startDB(String DB_PATH)
	{
		GraphDatabaseService graphdb = new GraphDatabaseFactory()
				.newEmbeddedDatabaseBuilder( DB_PATH )
				.setConfig( GraphDatabaseSettings.nodestore_mapped_memory_size, "100M" )
			    .setConfig( GraphDatabaseSettings.string_block_size, "600" )
			    .setConfig( GraphDatabaseSettings.array_block_size, "3000" )
			    .newGraphDatabase();

		registerShutdownHook( graphdb );
		
		return graphdb;
	}

	@Override
	public void stopDB(GraphDatabaseService graphDb)
	{
		if (graphDb != null)
		{
			graphDb.shutdown();
		}
	}
	
	private void registerShutdownHook(final GraphDatabaseService graphDb )
	{
	    // Registers a shutdown hook for the Neo4j instance so that it
	    // shuts down nicely when the VM exits (even if you "Ctrl-C" the
	    // running application).
	    Runtime.getRuntime().addShutdownHook( new Thread()
	    {
	        @Override
	        public void run()
	        {
	            graphDb.shutdown();
	        }
	    } );
	}
	

	private void travser2Json2(GraphDatabaseService graphDb, Node startNode, int maxdegree)
	{
		Traverser traverser = graphDb.traversalDescription()
	            .breadthFirst()
	            .relationships( RelTypes.KNOWS )
	            .evaluator(Evaluators.toDepth(maxdegree))
	            .traverse(startNode);
		List<Path> paths = new ArrayList<Path>();
		for(Path path : traverser)
		{
			if ((boolean) path.endNode().getProperty("isLike"))
			{
				paths.add(path);
			}
		}

		HashMap<Long, Node> nodes = new HashMap<Long, Node>();
		HashMap<Long, Relationship> edges = new HashMap<Long, Relationship>();
		for(Path path : paths)
		{
			for(Node node : path.nodes())
			{
				nodes.put(node.getId(), node);
			}
			for(Relationship edge : path.relationships())
			{
				edges.put(edge.getId(), edge);
			}
		}
		
		// add your code to convert nodes and edges to your JSON class.
		// ...
	}
	/**
	 * 
	 * @param graphDb
	 * @param nodeId
	 */
	private String travser2Json(GraphDatabaseService graphDb, Node startNode, int maxdegree)
	{
		StringBuilder sb = new StringBuilder();
		sb.append("{\n");
		sb.append("\"startNode\":\"" + startNode.getProperty( "UserName" ) + "\",\n");
		sb.append("\"maxdegree\":" + maxdegree + ",\n");
		
		HashMap<Node, Long> nodes = new HashMap<Node, Long>();
		Set<Relationship> edges = new HashSet<Relationship>();
		Traverser traverser = graphDb.traversalDescription()
	            .breadthFirst()
	            .relationships( RelTypes.KNOWS )
	            .evaluator(Evaluators.toDepth(maxdegree))
	            .traverse(startNode);
		Long idx = (long) 0;
		sb.append("\"nodes\":\n[\n");
		for (Node node : traverser.nodes())
		{
		    nodes.put(node, idx++);
		    sb.append("{");
		    sb.append(graphNode2String(node));
		    sb.append("},\n");
		}
		// remove the , of the last item
		sb.delete(sb.length()-2, sb.length());
		sb.append("\n],\n");

		sb.append("\"links\":[\n");
		for (Node friend : nodes.keySet())
		{
		    for (Relationship rel : friend.getRelationships())
		    {
		    	Node ffriend = rel.getOtherNode(friend);
		        if (nodes.containsKey(ffriend))
		        {
		            edges.add(rel);
				    sb.append("{\"source\":" + nodes.get(friend) + ",");
				    sb.append("\"target\":" + nodes.get(ffriend) + ",");
				    sb.append("\"value\":" + rdm.nextInt(10) + "},\n");
		        }
		    }
		}
		// remove the , of the last item
		sb.delete(sb.length()-2, sb.length());
		sb.append("\n]\n");
		sb.append("\"nodesnum\":" + nodes.size() + ",\n");
		sb.append("\"edgesnum\":" + edges.size() + "\n");
		sb.append("\n}");

		return sb.toString().replace("/", "\\");
	}
	

//	private static Traverser getFriends(GraphDatabaseService graphDb, final Node person)
//	{
//	    TraversalDescription td = graphDb.traversalDescription()
//	            .breadthFirst()
//	            .relationships( RelTypes.KNOWS )
//	            .evaluator( Evaluators.excludeStartPosition() );
//	    return td.traverse( person );
//	}

	/**
	 * Convert graph node to a string
	 * @param node - the graph node
	 * @return a string containing all properties and values
	 * @author Guodong Long
	 */
	private String graphNode2String(Node node)
	{
		StringBuilder sb = new StringBuilder();
		
		
		for(String prop : node.getPropertyKeys())
		{
			sb.append("\"" + prop + "\":");
			sb.append("\"" + node.getProperty(prop) + "\",");
		}
		// remove the , of the last item
		sb.delete(sb.length()-1, sb.length());
		return sb.toString();
	}
	


	enum RelTypes implements RelationshipType
	{
	    CONTAINED_IN, KNOWS
	}

	enum Direction implements RelationshipType
	{
	    OUTGOING
	}

}
