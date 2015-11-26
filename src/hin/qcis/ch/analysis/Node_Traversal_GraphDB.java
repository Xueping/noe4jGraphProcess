package hin.qcis.ch.analysis;

import hin.qcis.ch.domain.Graph;
import hin.qcis.ch.graphDB.GraphDBMgt_neo4j.Labels;
import hin.qcis.ch.graphDB.GraphDBMgt_neo4j.RelTypes;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.Random;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.traversal.Evaluators;
import org.neo4j.graphdb.traversal.Traverser;


public class Node_Traversal_GraphDB{


	
	public Graph queryEgoNetwork_Graph(GraphDatabaseService graphDBSvc,
			String propName, String propValue, int MAX_DEGREE) 
	{
		Graph graph = new Graph();
		try ( Transaction tx = graphDBSvc.beginTx() )
		{  
			for(Node node : graphDBSvc.findNodesByLabelAndProperty(Labels.CUSTOMER, propName, propValue))
			{
				graph = travser2Graph(graphDBSvc, node, MAX_DEGREE);
			}
			
			tx.success();
		}
		return graph;
	}	
	
	
	/**
	 * Given a customer, travese his/her ego social network to find all loyalty customer (the person who liked the company page). 
	 * @param graphDb
	 * @param startNode
	 * @param maxdegree
	 * @return
	 */
	private Graph travser2Graph(GraphDatabaseService graphDb, Node startNode, int maxdegree)
	{
		Traverser traverser = graphDb.traversalDescription()
	            .breadthFirst()
	            .relationships( RelTypes.KNOWS )
	            .evaluator(Evaluators.toDepth(maxdegree))
	            .traverse(startNode);
		List<Path> paths = new ArrayList<Path>();
		for(Path path : traverser)
		{
			if ((boolean) path.endNode().getProperty("IsLike"))
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
		Graph graph = new Graph();
		ArrayList<hin.qcis.ch.domain.Node> g_nodes = new ArrayList<hin.qcis.ch.domain.Node>();
		ArrayList<hin.qcis.ch.domain.Node> g_likeNodes = new ArrayList<hin.qcis.ch.domain.Node>();
		HashMap<Node, Long> idxNodes = new HashMap<Node, Long>();
		long idx = 0;
		for (Entry<Long, Node> entry : nodes.entrySet())
		{
			Node node = entry.getValue();
			hin.qcis.ch.domain.Node g_node = new hin.qcis.ch.domain.Node();
		    g_node.setUID((String)node.getProperty("UID"));
		    g_node.setUserName((String)node.getProperty("UserName"));
		    g_node.setPhotoSrc((String)node.getProperty("PhotoSrc"));
		    g_node.setProfilLink((String)node.getProperty("ProfilLink"));
		    if((boolean)node.getProperty("IsLike"))
		    	g_node.setIsLike("true");
		    else
		    	g_node.setIsLike("false");
		    g_nodes.add(g_node);
		    if((boolean)node.getProperty("IsLike"))
		    	g_likeNodes.add(g_node);
		    idxNodes.put(node, idx++);
		}
		graph.setNodes(g_nodes);
		
		ArrayList<hin.qcis.ch.domain.Link> g_links = new ArrayList<hin.qcis.ch.domain.Link>();
		for (Entry<Long, Relationship> entry : edges.entrySet())
		{
			Relationship edge = entry.getValue();
            hin.qcis.ch.domain.Link g_link = new hin.qcis.ch.domain.Link();
            g_link.setSource(idxNodes.get(edge.getStartNode()));
            g_link.setTarget(idxNodes.get(edge.getEndNode()));
            g_link.setSourceUID((String)edge.getStartNode().getProperty("UID"));
            g_link.setTargetUID((String)edge.getEndNode().getProperty("UID"));
            g_link.setValue(new Random().nextInt(10));
            g_links.add(g_link);
		}
		graph.setLinks(g_links);
		
		hin.qcis.ch.domain.Node gsn = new hin.qcis.ch.domain.Node();
		gsn.setUID((String)startNode.getProperty("UID"));
		gsn.setUserName((String)startNode.getProperty("UserName"));
		gsn.setPhotoSrc((String)startNode.getProperty("PhotoSrc"));
		gsn.setProfilLink((String)startNode.getProperty("ProfilLink"));
	    if((boolean)startNode.getProperty("IsLike"))
	    	gsn.setIsLike("true");
	    else
	    	gsn.setIsLike("false");
		graph.setStartNode(gsn);
		
		for(hin.qcis.ch.domain.Node n : g_likeNodes) {
			if(n.getUID()==(String)startNode.getProperty("UID")) {
				g_likeNodes.remove(n);
				break;
			}
		}
		graph.setLikeNodes(g_likeNodes);
		return graph;
	}

}