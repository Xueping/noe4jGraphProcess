package hin.qcis.ch.transform;

import hin.qcis.ch.graphDB.GraphDBMgt_neo4j;
import hin.qcis.ch.graphDB.GraphDBMgt_neo4j.Labels;
import hin.qcis.ch.graphDB.IGraphDBMgt;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

import javax.xml.bind.DatatypeConverter;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

public class ImageStorage {
	
	
	private IGraphDBMgt graphDriver;
	private GraphDatabaseService graphDBSvc;
	
	public ImageStorage(String DB_PATH){
		graphDriver = new GraphDBMgt_neo4j();
		graphDBSvc = graphDriver.startDB(DB_PATH);
	}
	
	public void stopDB(){
		graphDriver.stopDB(graphDBSvc);
	}
	
	
	
	public void process() throws FileNotFoundException, IOException
	{
		try ( Transaction tx = graphDBSvc.beginTx() )
		{
			Node node_category = graphDBSvc.createNode();
			node_category.setProperty("name", "picture");
			byte[] image = InputStreamToByte.inputStreamToByte(new FileInputStream("/Users/xuepingpeng/Pictures/public/DSC01676.JPG"));
			String encoded = DatatypeConverter.printBase64Binary(image);
			node_category.setProperty("image", encoded);
			node_category.addLabel(Labels.CATEGORY);
		    tx.success();
		}
	}
	
	
	public static void main(String[] args) throws FileNotFoundException, IOException{
		
		String DB_PATH = "/Users/xuepingpeng/Dropbox/projects/neo4j";
		ImageStorage have = new ImageStorage(DB_PATH);
		have.process();
		have.stopDB();
	}
	

}
