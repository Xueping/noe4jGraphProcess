package hin.qcis.ch.graphDB;

import java.util.HashMap;
import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.Schema;

import CrawlJSON2GraphDB.Main;
import CrawlJSON2GraphDB.DataModel.CrawlJSON.AllPersons;
import CrawlJSON2GraphDB.DataModel.CrawlJSON.Person;


public class GraphDB {
	
	//private String DB_PATH;
	private AllPersons allpersons;
	private GraphDatabaseService graphDBSvc;
	private int MAX_DEGREE = 1;
	
	public GraphDB(GraphDatabaseService gdb, String dbpath, AllPersons persons)
	{
		graphDBSvc = gdb;
		//DB_PATH = dbpath;
		allpersons = persons;
	}
	
	public void clean()
	{
		try ( Transaction tx = graphDBSvc.beginTx() )
		{
			GDBDelete.cleanDB(graphDBSvc);
			tx.success();
		}
		
		createIndex("Person", "UID");
	}
	
	public void init()
	{
		try ( Transaction tx = graphDBSvc.beginTx() )
		{
			GDBInsert.insertNodesLinks(graphDBSvc, allpersons.getHashMap());

		    tx.success();
		}
		
		createIndex("Person", "UID");
	}
	
	public void createIndex(String labelName, String propertyName)
	{
		IndexDefinition indexDefinition;
		try ( Transaction tx = graphDBSvc.beginTx() )
		{
		    Schema schema = graphDBSvc.schema();
		    indexDefinition = schema.indexFor( DynamicLabel.label( labelName ) )
		            .on( propertyName )
		            .create();

		    tx.success();
		}
		try ( Transaction tx = graphDBSvc.beginTx() )
		{
		    Schema schema = graphDBSvc.schema();
		    schema.awaitIndexOnline( indexDefinition, 10, TimeUnit.SECONDS );
		    tx.success();
		}
	}

	public void Analysis()
	{
		try ( Transaction tx = graphDBSvc.beginTx() )
		{   

//			for ( Node node :  GlobalGraphOperations.at(graphDb).getAllNodes() )
//		    {
//				int numOfFriends = this.outputTravser(graphDBSvc, node, MAX_DEGREE);
//				if (numOfFriends >= 1)
//				{
//					log.println("Node " + node.getId() + ", UID " + node.getProperty("UID") + ", # Friends " + numOfFriends);
//				}
//		    }
			for(Node node : graphDBSvc.findNodesByLabelAndProperty(Main.personLabel, "UID", "steve_knight_1656"))
//			for(Node node : queryNodeByProperty( graphDb, "Person", "UID", "gull_dukhtar_9674"))
		    {
		        System.out.println("Find node ID " + node.getId());
//		        GDBAnalysis.outputTravser(graphDBSvc, node, MAX_DEGREE);
		        GDBAnalysis.outputTravser2Visual(graphDBSvc, node, MAX_DEGREE);
		    }
			
			
//			Node neoNode = graphDBSvc.getNodeById(6057);
			
		    tx.success();
		}
	}

	public void createDatabase(GraphDatabaseService graphDBSvc, HashMap<String, Person> allpersons)
	{
		System.out.println(graphDBSvc);
		try ( Transaction tx = graphDBSvc.beginTx() )
		{
			GDBInsert.insertNodesLinks(graphDBSvc, allpersons);

		    tx.success();
		}
		
		createIndex(graphDBSvc, "Person", "UID");
	}
	
	public void createIndex(GraphDatabaseService graphDBSvc, String labelName, String propertyName)
	{
		IndexDefinition indexDefinition;
		try ( Transaction tx = graphDBSvc.beginTx() )
		{
		    Schema schema = graphDBSvc.schema();
		    indexDefinition = schema.indexFor( DynamicLabel.label( labelName ) )
		            .on( propertyName )
		            .create();

		    tx.success();
		}
		try ( Transaction tx = graphDBSvc.beginTx() )
		{
		    Schema schema = graphDBSvc.schema();
		    schema.awaitIndexOnline( indexDefinition, 10, TimeUnit.SECONDS );
		    tx.success();
		}
	}
	
}
