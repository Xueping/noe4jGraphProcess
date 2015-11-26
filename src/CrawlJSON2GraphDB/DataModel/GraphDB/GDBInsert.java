package CrawlJSON2GraphDB.DataModel.GraphDB;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;

import CrawlJSON2GraphDB.Main;
import CrawlJSON2GraphDB.DataModel.CrawlJSON.AllPersons;
import CrawlJSON2GraphDB.DataModel.CrawlJSON.Person;

public class GDBInsert {

	public static void insertNodesLinks(GraphDatabaseService graphDb, HashMap<String, Person> allpersons)
	{	
		// repeat for nodes
		for (Entry<String, Person> entry : allpersons.entrySet()) 
		{
		    Person person = entry.getValue();
		    if (person.getUID() == null)
		    {
		    	System.out.println("person is null!");
		    	continue;
		    }
		    
		    // ...
			Node node = graphDb.createNode();
			node.setProperty("UID", person.getUID());
			node.setProperty("UserName", person.getUserName());
			node.setProperty("PhotoSrc", person.getPhotoSrc());
			node.setProperty("ProfilLink", person.getProfileLink());
			node.setProperty("IsLike", person.isLike());
			node.addLabel(Main.personLabel);
			
		    person.setIndex(node.getId());
		    System.out.println("insert person " + node.getId() + ":" + person.getUID());
		}
		
		// repeat for links
		for (Entry<String, Person> entry : allpersons.entrySet()) 
		{
		    Person person = entry.getValue();
		    Node srcNode = graphDb.getNodeById(person.getIndex());
		    
		    if (person.getFriendList().size() > 0)
		    {
			    for(String friendUID : person.getFriendList())
			    {
			    	Person friend = allpersons.get(friendUID);
				    Node tgtNode = graphDb.getNodeById(friend.getIndex());
					RelationshipType knows = DynamicRelationshipType.withName( "KNOWS" );
					// To set properties on the relationship, use a properties map
					// instead of null as the last parameter.
					Relationship rl = srcNode.createRelationshipTo(tgtNode, knows);
//				    System.out.println("insert linkIdx " + rl.getId() + ":" + person.getIndex() + "--" + friend.getIndex());
			    }
		    }
		}
		
	}
	

	public static void batchInsertNodesLinks(GraphDatabaseService graphDb, AllPersons allpersons, String storeDir)
	{
		Map<String, String> config = new HashMap<>();
		config.put( "neostore.nodestore.db.mapped_memory", "90M" );
		BatchInserter inserter = BatchInserters.inserter(storeDir, config);
		Label personLabel = DynamicLabel.label( "Person" );
		inserter.createDeferredSchemaIndex( personLabel ).on( "UID" ).create();
		
		// repeat for nodes
		for (Entry<String, Person> entry : allpersons.getHashMap().entrySet()) {
		    Person person = entry.getValue();
		    if (person.getUID() == null)
		    {
		    	System.out.println("person is null!");
		    	continue;
		    }
		    
		    // ...
			Map<String, Object> properties = new HashMap<>();
			properties.put("UID", person.getUID());
			properties.put("UserName", person.getUserName());
			properties.put("PhotoSrc", person.getPhotoSrc());
			properties.put("ProfilLink", person.getProfileLink());
			long nodeIdx = inserter.createNode( properties, personLabel );
		    person.setIndex(nodeIdx);
		    System.out.println("insert person " + nodeIdx + ":" + person.getUID());
		}
		
		// repeat for links
		for (Entry<String, Person> entry : allpersons.getHashMap().entrySet()) {
		    String key = entry.getKey();
		    Person person = entry.getValue();
		    
		    if (person.getFriendList().size() > 0)
		    {
			    for(String friendUID : person.getFriendList())
			    {
			    	Person friend = allpersons.queryPersonByUID(friendUID);
					RelationshipType knows = DynamicRelationshipType.withName( "KNOWS" );
					// To set properties on the relationship, use a properties map
					// instead of null as the last parameter.
					long linkIdx = inserter.createRelationship( person.getIndex(), friend.getIndex(), knows, null );
				    System.out.println("insert linkIdx " + linkIdx + ":" + person.getIndex() + "--" + friend.getIndex());
			    }
		    }
		}

		inserter.shutdown();
		
	}
}
