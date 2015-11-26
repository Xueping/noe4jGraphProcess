package loadCSV;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;

import commons.GraphDBMgt_neo4j;
import commons.IGraphDBMgt;

public class LoadCSVFile {
	
	private IGraphDBMgt graphDriver;
	private GraphDatabaseService graphDBSvc;
	
	public LoadCSVFile(String DB_PATH){
		graphDriver = new GraphDBMgt_neo4j();
		graphDBSvc = graphDriver.startDB(DB_PATH);
	}
	
	public void stopDB(){
		graphDriver.stopDB(graphDBSvc);
	}
	
	public void loadNodes(String filePath){
		//create nodes
		File file = new File(filePath);
		String lebal = file.getName().split("-")[1].split("\\.")[0];
		String[] headers = null;
		try {
			String line = "";
			BufferedReader br = new BufferedReader(new FileReader(file));
			if((line = br.readLine()) != null){
				headers = line.split(",");
			}
			br.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		StringBuffer sb = new StringBuffer("");
		for(String header: headers){
			sb.append(header+": line."+header+",");
		}
		
		String fields = sb.toString().substring(0, sb.toString().length()-1);
		
		String sqlAuthor = "LOAD CSV WITH HEADERS FROM 'file:///"+filePath+"' AS line "
				+ "CREATE (:"+lebal+" { "+fields+"})";
		
		try ( Transaction tx = graphDBSvc.beginTx() )
		{
			graphDBSvc.execute(sqlAuthor);
		    tx.success();
		}
		
		try ( Transaction tx = graphDBSvc.beginTx() )
		{
			graphDBSvc.execute("CREATE INDEX ON :"+lebal+"("+headers[0]+");");
		    tx.success();
		}
	}
	
	
	
	public void loadRelationships(String filePath){
		//create relationships
		File file = new File(filePath);
		String lebal = file.getName().split("-")[1].split("\\.")[0];
		String[] headers = null;
		try {
			String line = "";
			BufferedReader br = new BufferedReader(new FileReader(file));
			if((line = br.readLine()) != null){
				headers = line.split(",");
			}
			br.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		StringBuffer sb = new StringBuffer("");
		for(String header: headers){
			sb.append(header+": line."+header+",");
		}
		
		String sqlAuthor = "LOAD CSV WITH HEADERS FROM 'file:///"+filePath+"' AS line "
				+ "MATCH (node1:"+headers[0].split("_")[0]+" {"+headers[0].split("_")[1]+": line."+headers[0]+"}) "
				+ "MATCH (node2:"+headers[1].split("_")[0]+" {"+headers[1].split("_")[1]+": line."+headers[1]+"}) "
				+ "MERGE (node1)-[:"+lebal+"]->(node2); ";
		
		
		try ( Transaction tx = graphDBSvc.beginTx() )
		{
			graphDBSvc.execute(sqlAuthor);
		    tx.success();
		}
		
	}
	
	public void match(){
		
//		Result result = graphDBSvc.execute( "match (n:Musics {Music: 'Love of Family'}) return n.Id, n.Year, n.Music" );
//		Result result = graphDBSvc.execute( "match (n:Artists {Name: 'ABBA'}) return n.Id, n.Year, n.Name" );
		Result result = graphDBSvc.execute( "match ((a:Artists)-[:RELEASE]-> (n:Musics {Music: 'Love of Family'})-[:PUBLISH]->(o:Organization)) return a.Name,o.Name" );
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
	
	
	public void parseZipFile(){
		try {
			ZipFile zipFile = new ZipFile("/home/xuepeng/uts/neo4j/rawData/rawData.zip");
			Enumeration<? extends ZipEntry> entries = zipFile.entries();

		    while(entries.hasMoreElements()){
		        ZipEntry entry = entries.nextElement();
		        
		        System.out.println(entry.getName());
//		        InputStream stream = zipFile.getInputStream(entry);
		    }
		    
		    zipFile.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
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
		
		String DB_PATH = "/home/xuepeng/uts/neo4j/csv-gdb";
		LoadCSVFile have = new LoadCSVFile(DB_PATH);
		List<String> fileList =  new ArrayList<String>();
		fileList.add("/home/xuepeng/uts/neo4j/rawData/node-Artists.csv");
		fileList.add("/home/xuepeng/uts/neo4j/rawData/node-Musics.csv");
		fileList.add("/home/xuepeng/uts/neo4j/rawData/edge-RELEASE.csv");
		fileList.add("/home/xuepeng/uts/neo4j/rawData/node-Organization.csv");
		fileList.add("/home/xuepeng/uts/neo4j/rawData/edge-PUBLISH.csv");
		
		
		for(String fp : fileList){
			File file = new File(fp);
			if(file.getName().startsWith("node-")){
				have.loadNodes(fp);
			}
		}
		
		for(String fp : fileList){
			File file = new File(fp);
			if(file.getName().startsWith("edge-")){
				have.loadRelationships(fp);
			}
		}
		
		have.process();
		have.stopDB();
		
	}
	

}
