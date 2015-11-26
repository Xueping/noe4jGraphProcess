package CrawlJSON2GraphDB;

import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

import org.json.simple.JSONObject;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;

import CrawlJSON2GraphDB.DataModel.CrawlJSON.AllPersons;
import CrawlJSON2GraphDB.DataModel.GraphDB.GraphDBDriver_neo4j;
import CrawlJSON2GraphDB.DataModel.GraphDB.IGraphDBDriver;
import CrawlJSON2GraphDB.DataOperation.Transform;

public class Main {

	public static Random rdm = new Random();
	public static Label personLabel = DynamicLabel.label( "Person" );
	public static String DB_PATH = System.getProperty("user.dir") + "/data/GraphDB4/";

	public static AllPersons glb_allpersons = new AllPersons();

	
	public static void main(String[] args) throws FileNotFoundException, IOException
	{
		// read json data to the middle variable -- AllPersons.
		String JSONFolder = System.getProperty("user.dir") + "/data/srcfiles/json4/";
		Main.glb_allpersons = Transform.Crawl2Var(JSONFolder);

		// transform middle variable to the JSON file for visualization.
		JSONObject graphJSON = Transform.Var2Visual(Main.glb_allpersons);
		String visual_file = System.getProperty("user.dir") + "/data/visualization/Graph_Visual.json";
		FileWriter fw = new FileWriter(visual_file);
		fw.write(graphJSON.toJSONString());
		fw.close();
		
		// transform middle variable to the Gephi file.
//		String gmlFile = System.getProperty("user.dir") + "/data/Gephi/" + "FB_CWB.gml";
//		GMLGraph gmlGraph = Transform.var2GML(main.glb_allpersons, 1000000);
//		FileWriter gmlFW = new FileWriter(gmlFile);
//		gmlFW.write(gmlGraph.toString());
//		gmlFW.close();
		
		// transform middle variable to the Graph database.
		IGraphDBDriver ga = new GraphDBDriver_neo4j();
		GraphDatabaseService graphDBSvc = ga.startDB(Main.DB_PATH);
		
		// create database
//		GraphDB gdb = new GraphDB(graphDBSvc, main.DB_PATH, main.glb_allpersons);
//		gdb.init();
		
		// GraphDBDriver
		//String sb = ga.queryEgoNetwork(graphDBSvc, "UID", "sayed.k.hamidi", 2);
		String sb2 = ga.queryUsers(graphDBSvc, "UserName", "sayed");
		String str = "\"aaa\n";
		char[] ch = str.toCharArray();
		System.out.println(ch);
		System.out.println(sb2);

		//
//		ExecutionEngine engine = new ExecutionEngine( graphDBSvc );
//		GDBSQL.execQuery(graphDBSvc, "");
//		Map<String, Object> params = new HashMap<String, Object>();
//		params.put( "UID", UID );
//		String query = "match (s {UID:\"daniellehall91\"})-[:KNOWS]-(l)-[:KNOWS]-(p {IsLike:false})-[:KNOWS]-(q) RETURN s,p,l,q limit 500";
//		GDBSQL.execQueryNodes(graphDBSvc, engine, query, null);
		
//		// analysis the graph
//		gdb.Analysis();
		
		ga.stopDB(graphDBSvc);
	}
	

}
