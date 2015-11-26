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

public class TransformNodeAndLink {
	
	private Map<String, Long> hireToKey = new HashMap<String, Long>();
	private Map<String, Long> branchToKey = new HashMap<String, Long>();
	private Map<String, Long> customerToKey = new HashMap<String, Long>();
	private Map<String, Long> fleetToKey = new HashMap<String, Long>();
	private Map<String, Long> categoryToKey = new HashMap<String, Long>();
	private IGraphDBMgt graphDriver;
	private GraphDatabaseService graphDBSvc;
	
	public TransformNodeAndLink(String DB_PATH){
		graphDriver = new GraphDBMgt_neo4j();
		graphDBSvc = graphDriver.startDB(DB_PATH);
	}
	
	public void stopDB(){
		graphDriver.stopDB(graphDBSvc);
	}
	
	public void transformHireMaster(){
		//create Customer nodes
		String sqlCustomer = "SELECT CUSTOMER_CODE,CUSTOMER_ID,CUSTOMER_DESC,CUSTOMER_NAME FROM hin.vwNode_Customer_Select;";
		try {
			Connection con = new SimpleConnetion().getConnection_EDW();
			PreparedStatement stm = con.prepareStatement(sqlCustomer);
			ResultSet rs = stm.executeQuery();
			while (rs.next()) {
				 // create hire node
				Node node_customer = graphDBSvc.createNode();
				node_customer.setProperty("CUSTOMER_CODE", rs.getString("CUSTOMER_CODE"));
				node_customer.setProperty("CUSTOMER_ID", rs.getString("CUSTOMER_ID"));
				node_customer.setProperty("CUSTOMER_DESC", rs.getString("CUSTOMER_DESC"));
				node_customer.setProperty("CUSTOMER_NAME", rs.getString("CUSTOMER_NAME"));
				node_customer.addLabel(Labels.CUSTOMER);
				customerToKey.put(rs.getString("CUSTOMER_CODE"), node_customer.getId());
			}
			stm.close();
			con.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		//create branch nodes
		String sqlBranch = "SELECT Branch_Code,BRANCH_DESC,BRANCH_NAME,BRANCH_AREA,BRANCH_STATE,BRANCH_BU FROM hin.vwNode_Branch_Select;";
		try {
			Connection con = new SimpleConnetion().getConnection_EDW();
			PreparedStatement stm = con.prepareStatement(sqlBranch);
			ResultSet rs = stm.executeQuery();
			while (rs.next()) {
				 // create hire node
				Node node_branch = graphDBSvc.createNode();
				node_branch.setProperty("Branch_Code", rs.getString("Branch_Code"));
				node_branch.setProperty("BRANCH_DESC", rs.getString("BRANCH_DESC"));
				node_branch.setProperty("BRANCH_NAME", rs.getString("BRANCH_NAME"));
				node_branch.setProperty("BRANCH_AREA", rs.getString("BRANCH_AREA"));
				node_branch.setProperty("BRANCH_STATE", rs.getString("BRANCH_STATE"));
				node_branch.setProperty("BRANCH_BU", rs.getString("BRANCH_BU"));
				node_branch.addLabel(Labels.BRANCH);
				branchToKey.put(rs.getString("Branch_Code"), node_branch.getId());
			}
			stm.close();
			con.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}	
		
		String sqlQueryStr = "SELECT Hire_No,Cust_Code,Branch_Code,Site,hire_revenue FROM hin.vwHireMast_Select;";
		try {
			Connection con = new SimpleConnetion().getConnection_EDW();
			PreparedStatement stm = con.prepareStatement(sqlQueryStr);
			ResultSet rs = stm.executeQuery();
			while (rs.next()) {
				 // create hire node
				Node node_hire = graphDBSvc.createNode();
				node_hire.setProperty("Hire_No", rs.getString("Hire_No"));
				node_hire.setProperty("Cust_Code", rs.getString("Cust_Code"));
				node_hire.setProperty("Branch_Code", rs.getString("Branch_Code"));
				node_hire.setProperty("Site", rs.getString("Site")!=null?rs.getString("Site"):"null");
				node_hire.addLabel(Labels.HIREMASTER);
				hireToKey.put(rs.getString("Hire_No"), node_hire.getId());
				
				 // create revenue node
				Node node_revenue = graphDBSvc.createNode();
				node_revenue.setProperty("Revenue_No", rs.getString("Hire_No"));
				node_revenue.setProperty("Revenue", rs.getString("hire_revenue"));
				node_revenue.addLabel(Labels.REVENUE);
				
				//create relationship between hire and revenue
				RelationshipType have = RelTypes.HAVE;
				node_hire.createRelationshipTo(node_revenue, have);
				
				//create relationship between customer and hire
				RelationshipType hire = RelTypes.HIRE;
				Node customerNode = graphDBSvc.getNodeById(customerToKey.get(rs.getString("Cust_Code"))) ;
				customerNode.createRelationshipTo(node_hire, hire);
				
				//create relationship between hire and branch
				RelationshipType from = RelTypes.FROM;
				Node branchNode = graphDBSvc.getNodeById(branchToKey.get(rs.getString("Branch_Code"))) ;
				node_hire.createRelationshipTo(branchNode, from);
				
			}
			stm.close();
			con.close();
			System.out.println(hireToKey.size());
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void transformHireDetail(){
		//create Fleet nodes
		String sqlFleet = "SELECT Fleet_No,Description,BranchFrom,Fleet_Type,Fleet_Model FROM hin.vwNode_Fleet_Select;";
		try {
			Connection con = new SimpleConnetion().getConnection_EDW();
			PreparedStatement stm = con.prepareStatement(sqlFleet);
			ResultSet rs = stm.executeQuery();
			while (rs.next()) {
				 // create fleet node
				Node node_fleet = graphDBSvc.createNode();
				node_fleet.setProperty("Fleet_No", rs.getString("Fleet_No"));
				node_fleet.setProperty("Description", rs.getString("Description")!=null?rs.getString("Description"):"null");
				node_fleet.setProperty("BranchFrom", rs.getString("BranchFrom"));
				node_fleet.setProperty("Fleet_Type", rs.getString("Fleet_Type"));
				node_fleet.setProperty("Fleet_Model", rs.getString("Fleet_Model"));
				node_fleet.addLabel(Labels.FLEET);
				fleetToKey.put(rs.getString("Fleet_No"), node_fleet.getId());
			}
			stm.close();
			con.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		//create Category nodes
		String sqlCategory = "SELECT Fleet_Type_Code,Fleet_Model_Code,fleet_pricegroup_code,fleet_category_code FROM hin.vwNode_Category_Select;";
		try {
			Connection con = new SimpleConnetion().getConnection_EDW();
			PreparedStatement stm = con.prepareStatement(sqlCategory);
			ResultSet rs = stm.executeQuery();
			while (rs.next()) {
				 // create category node
				Node node_category = graphDBSvc.createNode();
				node_category.setProperty("Fleet_Type_Code", rs.getString("Fleet_Type_Code"));
				node_category.setProperty("Fleet_Model_Code", rs.getString("Fleet_Model_Code"));
				node_category.setProperty("fleet_pricegroup_code", rs.getString("fleet_pricegroup_code"));
				node_category.setProperty("fleet_category_code", rs.getString("fleet_category_code"));
				node_category.addLabel(Labels.CATEGORY);
				categoryToKey.put(rs.getString("Fleet_Model_Code"), node_category.getId());
				
			}
			stm.close();
			con.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}	
		
		String sqlQueryStr = "SELECT Hire_No,Line_No,Machine_ID,Qty,Price,LineTot,PlantModel FROM hin.vwhireDet_Select;";
		try {
			Connection con = new SimpleConnetion().getConnection_EDW();
			PreparedStatement stm = con.prepareStatement(sqlQueryStr);
			ResultSet rs = stm.executeQuery();
			while (rs.next()) {
				 // create hiredetail node
				Node node_hireDet = graphDBSvc.createNode();
				node_hireDet.setProperty("Hire_No", rs.getString("Hire_No"));
				node_hireDet.setProperty("Line_No", rs.getString("Line_No"));
				node_hireDet.setProperty("Machine_ID", rs.getString("Machine_ID")!=null?rs.getString("Machine_ID"):"null");
				node_hireDet.setProperty("Qty", rs.getString("Qty")!=null?rs.getString("Qty"):"null");
				node_hireDet.setProperty("Price", rs.getString("Price")!=null?rs.getString("Price"):"null");
				node_hireDet.setProperty("LineTot", rs.getString("LineTot")!=null?rs.getString("LineTot"):"null");
				node_hireDet.setProperty("PlantModel", rs.getString("PlantModel")!=null?rs.getString("PlantModel"):"null");
				node_hireDet.addLabel(Labels.HIREDETAIL);
				
				//create relationship between hireMaster and HireDet
				RelationshipType contain = RelTypes.CONTAIN;
				Node HireMast_Node = graphDBSvc.getNodeById(hireToKey.get(rs.getString("Hire_No"))) ;
				HireMast_Node.createRelationshipTo(node_hireDet, contain);
				
				//create relationship between hireDet and Fleet
				RelationshipType is = RelTypes.IS;
				Long  fleet_key = fleetToKey.get(rs.getString("Machine_ID")!=null?rs.getString("Machine_ID"):"null");
				if(fleet_key!=null){
					Node fleet_Node = graphDBSvc.getNodeById(fleet_key);
					node_hireDet.createRelationshipTo(fleet_Node, is);
				}
				
				//create relationship between Fleet and category
				RelationshipType under = RelTypes.UNDER;
				Long category_key = categoryToKey.get(rs.getString("PlantModel")!=null?rs.getString("PlantModel"):"null");
				if(fleet_key!=null&&category_key!=null){
					Node category_Node = graphDBSvc.getNodeById(category_key);
					Node fleet_Node = graphDBSvc.getNodeById(fleet_key);
					fleet_Node.createRelationshipTo(category_Node, under);
				}
			}
			stm.close();
			con.close();
			System.out.println(hireToKey.size());
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void process()
	{
		try ( Transaction tx = graphDBSvc.beginTx() )
		{
			transformHireMaster();
			transformHireDetail();
		    tx.success();
		}
	}
	
	
	public static void main(String[] args){
		
		String DB_PATH = "C:\\Dropbox\\Dropbox\\projects\\HIN\\HIN_GDB";
		TransformNodeAndLink have = new TransformNodeAndLink(DB_PATH);
		have.process();
		have.stopDB();
		
	}
	

}
