package CrawlJSON2GraphDB.DataModel.GraphDB;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.neo4j.cypher.ExecutionEngine;
import org.neo4j.cypher.ExecutionResult;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

import CrawlJSON2GraphDB.DataModel.CrawlJSON.Person;

public class GDBQuery {
	
	/**
	 * Match node with given properties.
	 * @param node
	 * @param label
	 * @param properties
	 * @return
	 */
	public static boolean matchNode(Node node, Label label, HashMap<String, String> properties)
	{
		if (!node.hasLabel(label))
		{
			return false;
		}
		for (Entry<String, String> entry : properties.entrySet()) {
			String prop = entry.getKey();
			String value = entry.getValue();
			if (!node.hasProperty(prop))
			{
				return false;
			}
			else if(!node.getProperty(prop).toString().equalsIgnoreCase(value))
			{
				return false;
			}
		}
		return true;
	}
	
	/**
	 * Match link with given properties
	 * @param link
	 * @param properties
	 * @return
	 */
	public static boolean matchLink(Relationship link, HashMap<String, String> properties)
	{
		for (Entry<String, String> entry : properties.entrySet()) {
			String prop = entry.getKey();
			String value = entry.getValue();
			if (!link.hasProperty(prop))
			{
				return false;
			}
			else if(!link.getProperty(prop).toString().equalsIgnoreCase(value))
			{
				return false;
			}
		}
		return true;
	}

	/**
	 * Match two nodes
	 * @param node1
	 * @param node2
	 * @return
	 */
	public static boolean matchNode(Node node1, Node node2)
	{
		if (node1.equals(node2))
		{
			return true;
		}
		
		// match labels
		List<Label> arLabels1 = new ArrayList<Label>();
		for(Label label1 : node1.getLabels())
		{
			arLabels1.add(label1);
		}
		List<Label> arLabels2 = new ArrayList<Label>();
		for(Label label1 : node1.getLabels())
		{
			arLabels2.add(label1);
		}
		if (!arLabels1.equals(arLabels2))
		{
			return false;
		}
		
		// match properties
		List<String> arProps1 = new ArrayList<String>();
		List<String> arVals1 = new ArrayList<String>();
		for(String prop : node1.getPropertyKeys())
		{
			arProps1.add(prop);
			arVals1.add(node1.getProperty(prop).toString());
		}
		List<String> arProps2 = new ArrayList<String>();
		List<String> arVals2 = new ArrayList<String>();
		for(String prop : node2.getPropertyKeys())
		{
			arProps2.add(prop);
			arVals2.add(node2.getProperty(prop).toString());
		}

		if (!arProps1.equals(arProps2))
		{
			return false;
		}
		if (!arVals1.equals(arVals2))
		{
			return false;
		}
		
		return true;
	}
	
	/**
	 * Match two links (relationships)
	 * @param link1
	 * @param link2
	 * @return
	 */
	public static boolean matchLink(Relationship link1, Relationship link2)
	{
		if (link1.equals(link2))
		{
			return true;
		}
		
		// match properties
		List<String> arProps1 = new ArrayList<String>();
		List<String> arVals1 = new ArrayList<String>();
		for(String prop : link1.getPropertyKeys())
		{
			arProps1.add(prop);
			arVals1.add(link1.getProperty(prop).toString());
		}
		List<String> arProps2 = new ArrayList<String>();
		List<String> arVals2 = new ArrayList<String>();
		for(String prop : link2.getPropertyKeys())
		{
			arProps2.add(prop);
			arVals2.add(link2.getProperty(prop).toString());
		}

		if (!arProps1.equals(arProps2))
		{
			return false;
		}
		if (!arVals1.equals(arVals2))
		{
			return false;
		}
		
		return true;
	}
}
