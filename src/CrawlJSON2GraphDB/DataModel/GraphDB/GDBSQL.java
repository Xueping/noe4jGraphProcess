package CrawlJSON2GraphDB.DataModel.GraphDB;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.PropertyContainer;
import org.w3c.dom.Node;

public class GDBSQL {

	public static ExecutionResult execQuery(GraphDatabaseService graphDb, String UID)
	{
		ExecutionEngine engine = new ExecutionEngine( graphDb );
		Map<String, Object> params = new HashMap<String, Object>();
		params.put( "UID", UID );
		String query = "match (s {UID:\"daniellehall91\"})-[:KNOWS]-(l)-[:KNOWS]-(p {IsLike:false})-[:KNOWS]-(q) RETURN s,p,l,q limit 500";
		ExecutionResult result = engine.execute( query );
		String rows = "\"hello\"\n";
		for ( Map<String, Object> row : result)
		{
		    for ( Entry<String, Object> column : row.entrySet() )
		    {
		        rows += column.getKey() + ": " + column.getValue() + "; ";
		    }
		    rows += "\n";
		}
		System.out.println(rows);
		return result;
	}

	public static ExecutionResult execQuery(ExecutionEngine engine, String query, Map<String, Object> params)
	{
		ExecutionResult result = engine.execute(query, params);
		String rows = "";
		for ( Map<String, Object> row : result)
		{
		    for ( Entry<String, Object> column : row.entrySet() )
		    {
		        rows += column.getKey() + ": " + column.getValue() + "; ";
		    }
		    rows += "\n";
		}
		System.out.println(rows);
		return result;
	}

	public static HashMap<Long, Node> execQueryNodes(GraphDatabaseService graphDb, ExecutionEngine engine, String query, Map<String, Object> params)
	{
		ExecutionResult result = engine.execute(query, params);
		HashMap<Long, Node> nodes = new HashMap<Long, Node>();
		for ( Map<String, Object> row : result)
		{
		    for ( Entry<String, Object> column : row.entrySet() )
		    {
		    	String value = column.getValue().toString();
		    	int beginIndex = value.indexOf("[");
		    	int endIndex = value.indexOf("]");
		    	long nodeId = Long.valueOf(value.substring(beginIndex+1, endIndex-1));
		    	System.out.println(nodeId);
		    	Node node = (Node) graphDb.getNodeById(nodeId);
		        nodes.put(nodeId, node);
		    }
		}
		
		for( Entry<Long, Node> entry : nodes.entrySet())
		{
			Node node = entry.getValue();
			System.out.println(entry.getKey() + ":" + graphNode2String(node));
		}
		return nodes;
	}
	

	private static String graphNode2String(Node node)
	{
		StringBuilder sb = new StringBuilder();
		
		
		for(String prop : ((PropertyContainer) node).getPropertyKeys())
		{
			sb.append("\"" + prop + "\":");
			sb.append("\"" + ((PropertyContainer) node).getProperty(prop) + "\",");
		}
		// remove the , of the last item
		sb.delete(sb.length()-1, sb.length());
		
		return sb.toString();
	}
	
	
}
