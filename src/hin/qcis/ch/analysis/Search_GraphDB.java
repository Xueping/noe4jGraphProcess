package hin.qcis.ch.analysis;

import hin.qcis.ch.domain.SearchResult;

import java.util.ArrayList;
import java.util.List;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.tooling.GlobalGraphOperations;


public class Search_GraphDB{

	//search query involved in a certain property in GraphDb 
	public List<SearchResult> queryUsersByObjects(GraphDatabaseService graphDBSvc, String propName, String propValue){
		
		List<SearchResult> srs = new ArrayList<SearchResult>();
		try ( Transaction tx = graphDBSvc.beginTx() )
		{  
			for ( Node node :  GlobalGraphOperations.at(graphDBSvc).getAllNodes() )
			{
				String nodeValue = node.getProperty(propName).toString();
				if (nodeValue.contains(propValue))
				{
					SearchResult sr = new SearchResult();
					sr.setUid((String)node.getProperty("UID"));
					sr.setUserName((String)node.getProperty("UserName"));
					srs.add(sr);
				}
			}
			tx.success();
		}
		return srs;
	}
	
}