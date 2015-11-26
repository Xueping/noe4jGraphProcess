package metapath.graph;

import java.util.Map;
import java.util.Map.Entry;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;

import commons.GraphDBMgt_neo4j;
import commons.IGraphDBMgt;

public class ExtractSubGraph {
	
	private IGraphDBMgt graphDriver;
	private GraphDatabaseService graphDBSvc;
	
	public ExtractSubGraph(String DB_PATH){
		graphDriver = new GraphDBMgt_neo4j();
		graphDBSvc = graphDriver.startDB(DB_PATH);
	}
	
	public void stopDB(){
		graphDriver.stopDB(graphDBSvc);
	}
	
	
	public void newSubGraph(){
		
		//run the query based on the given metapath
		Result result = graphDBSvc.execute( "match ((f:Artists)-[:RELEASE]-> (n:Musics)-[:PUBLISH]->(l:Organization)) return f,l" );
		
		//new graph database
		String DB_PATH = "/home/xuepeng/uts/neo4j/newGDB";
		
		IGraphDBMgt subGraphDriver = new GraphDBMgt_neo4j();
		GraphDatabaseService subGraphDBSvc = subGraphDriver.startDB(DB_PATH);

		try ( Transaction tx = subGraphDBSvc.beginTx() )
		{
			//iterate the results
			while ( result.hasNext() )
		    {
		        Map<String,Object> row = result.next();
		        Entry<String,Object> column1 = (Entry<String,Object> )row.entrySet().toArray()[0];
		        
		        //get the first node
		        Node node1 = (Node)column1.getValue();
		        //get the Label of first node
		        Label label1 = null;
		        for(Label label : node1.getLabels()){
		        	label1 = label;
		        }
		        
		        //create first new node in new graph database
		        Node newNode1 = subGraphDBSvc.createNode();
		        
		        //set properties
		        for ( Map.Entry<String, Object> entry : node1.getAllProperties().entrySet() )
		        {
		        	newNode1.setProperty(entry.getKey(), entry.getValue().toString());
		        }
		        newNode1.addLabel(label1);
		        
		        //get the second node
		        Entry<String,Object> column2 = (Entry<String,Object> )row.entrySet().toArray()[1];
		        Node node2 = (Node)column2.getValue();
		        Label label2 = null;
		        for(Label label : node2.getLabels()){
		        	label2 = label;
		        }
		        
		        Node newNode2 = subGraphDBSvc.createNode();
		        for ( Map.Entry<String, Object> entry : node2.getAllProperties().entrySet() )
		        {
		        	newNode2.setProperty(entry.getKey(), entry.getValue().toString());
		        }
		        newNode2.addLabel(label2);
		        
		        //create the relationship between two nodes
		        RelationshipType rType=new RelationshipTypeImpl("test");
		        newNode1.createRelationshipTo(newNode2, rType);
		    }
		    tx.success();
		}
		
		subGraphDriver.stopDB(subGraphDBSvc);
	}
	
	public void match(){
		
		Result result = graphDBSvc.execute( "match ((a:Artists)-[:test]->(o:Organization)) return a.Name,o.Name" );
		String rows = "";
		while ( result.hasNext() )
	    {
			
	        Map<String,Object> row = result.next();
	        for ( Entry<String,Object> column : row.entrySet() )
	        {
	            rows += column.getKey() + ": " + column.getValue() + "; ";
	        }
	        rows += "\n";
	    }
		
		System.out.print(rows);
		
	}
	
	public void process()
	{
		try ( Transaction tx = graphDBSvc.beginTx() )
		{
//			loadNodes(filePath);
//			loadRelationships(filePath);
			match();
		    tx.success();
		}
	}
	
	
	public static void main(String[] args){
		
//		String DB_PATH = "/home/xuepeng/uts/neo4j/csv-gdb";
		String DB_PATH = "/home/xuepeng/uts/neo4j/newGDB";
		ExtractSubGraph have = new ExtractSubGraph(DB_PATH);

		have.process();
		have.stopDB();
		
	}
	

}
