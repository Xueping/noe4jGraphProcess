package CrawlJSON2GraphDB.DataOperation;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Random;
import java.util.Map.Entry;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import CrawlJSON2GraphDB.Main;
import CrawlJSON2GraphDB.DataModel.CrawlJSON.AllPersons;
import CrawlJSON2GraphDB.DataModel.CrawlJSON.Person;
import CrawlJSON2GraphDB.DataModel.Gephi.GMLGraph;

public class Transform {

	public static int MAX_NODE = 1000;
	

	/**
	 * Read json files and store them into a static middle variable -- AllPersons.
	 * @param JSONFolder
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	public static AllPersons Crawl2Var(String JSONFolder) throws FileNotFoundException, IOException
	{
		AllPersons allpersons = new AllPersons();
		File folder = new File(JSONFolder);
		for(File jsonFile : folder.listFiles())
		{
			Person person = new Person(jsonFile, allpersons);

			if(allpersons.addPersonWithChk(person) == 1)
			{
//				System.out.println("**add person " + person.getUID());
			}
			else
			{
//				System.out.println("--repeat person " + person.getUID());
			}
		}

		System.out.println("Total " + allpersons.getPersonCount() + " persons added!");
		
		return allpersons;
	}
	
	/**
	 * Transform the static middle variable - AllPersons - to the JSON file for visualization.
	 * @param allPersons
	 * @return
	 */
	public static JSONObject Var2Visual(AllPersons allpersons)
	{
	    JSONObject jo = new JSONObject();

	    JSONArray nodes = new JSONArray();
	    int index = 0;
		for (Entry<String, Person> entry : allpersons.getHashMap().entrySet()) {
		    Person person = entry.getValue();
		    if (person.getUID() == null)
		    {
		    	System.out.println("person is null!");
		    	continue;
		    }
		    person.setIndex(index++);
		    
		    // ...
		    JSONObject node = new JSONObject();
		    node.put("UID", person.getUID());
		    node.put("UserName", person.getUserName());
		    node.put("PhotoSrc", person.getPhotoSrc());
		    node.put("ProfilLink", person.getProfileLink());
		    nodes.add(node);
		    
		}
	    JSONArray links = new JSONArray();
		for (Entry<String, Person> entry : allpersons.getHashMap().entrySet()) {
		    String key = entry.getKey();
		    Person person = entry.getValue();
		    
		    if (person.getFriendList().size() > 0)
		    {
			    for(String friendUID : person.getFriendList())
			    {
			    	Person friend = allpersons.queryPersonByUID(friendUID);
				    JSONObject link = new JSONObject();
			    	link.put("source", person.getIndex());
			    	link.put("target", friend.getIndex());
			    	link.put("value", Main.rdm.nextInt());
				    links.add(link);
			    }
		    }
		}
		
	    jo.put("nodes", nodes);
	    jo.put("links", links);
	    
	    return jo;
	}
	
	/**
	 * Convert the variable to GML file for Gephi.
	 * @param allpersons
	 * @return
	 */
	public static GMLGraph var2GML(AllPersons allpersons, int MAX_ID)
	{
		GMLGraph graph = new GMLGraph();

		long index = 0;
		for (Entry<String, Person> entry : allpersons.getHashMap().entrySet())
		{
			if (index > MAX_ID)
			{
				continue;
			}
			Person person = entry.getValue();
		    person.setIndex(index++);
			graph.addNode(person.getUID());
		}

		for (Entry<String, Person> entry : allpersons.getHashMap().entrySet())
		{
			Person person = entry.getValue();
			if (person.getIndex() > MAX_ID)
			{
				continue;
			}
		    if (person.getFriendList().size() > 0)
		    {
			    for(String friendUID : person.getFriendList())
			    {
			    	Person friend = allpersons.queryPersonByUID(friendUID);

					if (friend.getIndex() > MAX_ID)
					{
						continue;
					}
				    graph.addUndirectedLink(person.getIndex(), friend.getIndex(), Main.rdm.nextDouble());
			    }
		    }
		}
		return graph;
	}
		
}
