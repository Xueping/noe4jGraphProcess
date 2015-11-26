package table.to.graph;



import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

import commons.GraphDBMgt_neo4j;
import commons.GraphDBMgt_neo4j.Labels;
import commons.GraphDBMgt_neo4j.RelTypes;
import commons.IGraphDBMgt;
import db.connection.SimpleConnetion;

public class TransformNodeAndLink {
	
	private Map<String, Long> authorToKey = new HashMap<String, Long>();
	private Map<String, Long> paperToKey = new HashMap<String, Long>();
	private Map<String, Long> conferenceToKey = new HashMap<String, Long>();
	private IGraphDBMgt graphDriver;
	private GraphDatabaseService graphDBSvc;
	
	public TransformNodeAndLink(String DB_PATH){
		graphDriver = new GraphDBMgt_neo4j();
		graphDBSvc = graphDriver.startDB(DB_PATH);
	}
	
	public void stopDB(){
		graphDriver.stopDB(graphDBSvc);
	}
	
	public void transform(){
		//create author nodes
		String sqlAuthor = "SELECT distinct name from author;";
		try {
			Connection con = new SimpleConnetion().getConnection_DBLP_MySQL();
			PreparedStatement stm = con.prepareStatement(sqlAuthor);
			ResultSet rs = stm.executeQuery();
			while (rs.next()) {
				 // create hire node
				Node node_author = graphDBSvc.createNode();
				node_author.setProperty("NAME", rs.getString("name"));
				node_author.addLabel(Labels.AUTHOR);
				authorToKey.put(rs.getString("name"), node_author.getId());
			}
			stm.close();
			con.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		System.out.println("Finish creating author nodes......");
		
		//create conference nodes
		String sqlConf = "SELECT distinct conference FROM paper;";
		try {
			Connection con = new SimpleConnetion().getConnection_DBLP_MySQL();
			PreparedStatement stm = con.prepareStatement(sqlConf);
			ResultSet rs = stm.executeQuery();
			while (rs.next()) {
				 // create hire node
				Node node_Conf = graphDBSvc.createNode();
				node_Conf.setProperty("NAME", rs.getString("conference"));
				node_Conf.addLabel(Labels.CONFERENCE);
				conferenceToKey.put(rs.getString("conference"), node_Conf.getId());
			}
			stm.close();
			con.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}	
		
		System.out.println("Finish creating conference nodes......");
		
		String sqlQueryStr = "SELECT title, year,conference,paper_key FROM paper;";
		try {
			Connection con = new SimpleConnetion().getConnection_DBLP_MySQL();
			PreparedStatement stm = con.prepareStatement(sqlQueryStr);
			ResultSet rs = stm.executeQuery();
			while (rs.next()) {
				 // create paper nodes
				Node node_paer = graphDBSvc.createNode();
				node_paer.setProperty("TITLE", rs.getString("title"));
				node_paer.setProperty("YEAR", rs.getString("year"));
				node_paer.setProperty("KEY", rs.getString("paper_key"));
				node_paer.addLabel(Labels.PAPER);
				paperToKey.put(rs.getString("paper_key"), node_paer.getId());
				
				//create relationship between conference and paper
//				Node conference = graphDBSvc.getNodeById(conferenceToKey.get(rs.getString("conference"))) ;
//				node_paer.createRelationshipTo(conference, RelTypes.PUBLISH);
				
				Long  conf_key = conferenceToKey.get(rs.getString("conference")!=null?rs.getString("conference"):"null");
				if(conf_key!=null){
					Node conference = graphDBSvc.getNodeById(conf_key);
					node_paer.createRelationshipTo(conference, RelTypes.PUBLISH);
				}
			}
			stm.close();
			con.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		System.out.println("Finish creating relationship between paper and conference ......");
		
		String sqlAuthor_to_Paper = "SELECT name,paper_key FROM author;";
		try {
			Connection con = new SimpleConnetion().getConnection_DBLP_MySQL();
			PreparedStatement stm = con.prepareStatement(sqlAuthor_to_Paper);
			ResultSet rs = stm.executeQuery();
			while (rs.next()) {
				 
				Long  author_key = authorToKey.get(rs.getString("name")!=null?rs.getString("name"):"null");
				Long  paper_key = paperToKey.get(rs.getString("paper_key")!=null?rs.getString("paper_key"):"null");
				if(author_key!=null&&paper_key!=null){
					Node node_author = graphDBSvc.getNodeById(author_key) ;
					Node node_paer = graphDBSvc.getNodeById(paper_key) ;
					node_author.createRelationshipTo(node_paer, RelTypes.WRITE);
				}
				
			}
			stm.close();
			con.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		System.out.println("Finish creating relationship between paper and author ......");
	}
	
	
	public void process()
	{
		try ( Transaction tx = graphDBSvc.beginTx() )
		{
			transform();
		    tx.success();
		}
	}
	
	
	public static void main(String[] args){
		
		String DB_PATH = "/home/xuepeng/uts/neo4j/dblp_graph";
		TransformNodeAndLink have = new TransformNodeAndLink(DB_PATH);
		have.process();
		have.stopDB();
		
	}
	

}
