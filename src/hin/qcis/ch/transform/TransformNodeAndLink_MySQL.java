package hin.qcis.ch.transform;

import hin.qcis.ch.dbconnection.SimpleConnetion;
import hin.qcis.ch.graphDB.GraphDBMgt_neo4j;
import hin.qcis.ch.graphDB.GraphDBMgt_neo4j.Labels;
import hin.qcis.ch.graphDB.GraphDBMgt_neo4j.RelTypes;
import hin.qcis.ch.graphDB.IGraphDBMgt;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;

public class TransformNodeAndLink_MySQL {
	
	private Map<String, Long> userToKey = new HashMap<String, Long>();
	private IGraphDBMgt graphDriver;
	private GraphDatabaseService graphDBSvc;
	
	public TransformNodeAndLink_MySQL(String DB_PATH){
		graphDriver = new GraphDBMgt_neo4j();
		graphDBSvc = graphDriver.startDB(DB_PATH);
	}
	
	public void stopDB(){
		graphDriver.stopDB(graphDBSvc);
	}
	
	public void transformUser(){
		//create user nodes
		
		String userQuery = "" +
				"select id,name,image,jobid,src from "+

"(SELECT  id ,name ,image,jobid,'user' as src FROM ( SELECT  @row_num := IF(@prev_value=o.fid,@row_num+1,1) AS RowNumber ,o.fid as id ,o.name ,o.imagesrc as image ,o.jobid ,@prev_value := o.fid"
 +"       FROM users o, (SELECT @row_num := 1) x, (SELECT @prev_value := '') y ORDER BY o.fid, o.jobid DESC"
 +"    ) subquery WHERE RowNumber = 1)aa UNION ALL (SELECT  id ,name ,image,jobid, 'friend' as src"
 +" FROM ( SELECT  @row_num := IF(@prev_value=o.ffid,@row_num+1,1) AS RowNumber ,o.ffid as id ,o.ffname as name ,o.fimglink as image ,o.jobid ,@prev_value := o.ffid"
 +"       FROM friendship o,  (SELECT @row_num := 1) x,  (SELECT @prev_value := '') y ORDER BY o.ffid, o.jobid DESC"
 +"    ) subquery  WHERE RowNumber = 1 and id not in (select distinct fid from users));";
		try {
			Connection con = new SimpleConnetion().getConnection_MySQL();
			PreparedStatement stm = con.prepareStatement(userQuery);
			ResultSet rs = stm.executeQuery();
			while (rs.next()) {
				 // create hire node
				Node node= graphDBSvc.createNode();
				node.setProperty("id", rs.getString("id"));
				node.setProperty("name", rs.getString("name"));
				node.setProperty("image", rs.getString("image"));
				node.setProperty("jobid", rs.getString("jobid"));
				node.setProperty("src", rs.getString("src"));
				node.addLabel(Labels.Person);
				userToKey.put(rs.getString("id"), node.getId());
			}
			stm.close();
			con.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		
		//create friendship relation
		String friendship = "select distinct fid,ffid from friendship;";
		try {
			Connection con = new SimpleConnetion().getConnection_MySQL();
			PreparedStatement stm = con.prepareStatement(friendship);
			ResultSet rs = stm.executeQuery();
			while (rs.next()) {
				//create relationship between hireMaster and HireDet
				RelationshipType contain = RelTypes.FRIEND;
				Node originalUser = graphDBSvc.getNodeById(userToKey.get(rs.getString("fid"))) ;
				Node friend 	  = graphDBSvc.getNodeById(userToKey.get(rs.getString("ffid"))) ;
				originalUser.createRelationshipTo(friend, contain);
			}
			stm.close();
			con.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}	
	}
	
	
	
	public void process()
	{
		try ( Transaction tx = graphDBSvc.beginTx() )
		{
			transformUser();
		    tx.success();
		}
	}
	
	
	public static void main(String[] args){
		
		String DB_PATH = "/Users/xuepingpeng/Documents/projects/socialmedia/data/fb_graph_v02";
		TransformNodeAndLink_MySQL have = new TransformNodeAndLink_MySQL(DB_PATH);
		have.process();
		have.stopDB();
		
	}
	

}
