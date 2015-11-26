package hin.qcis.ch.dbconnection;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;


public class SelectSample {
	
	public static void main(String[] args) {
		
		System.out.print(System.getProperty("java.library.path"));
	
		String sqlQueryStr = "SELECT u.fid, Name AS fname, ffid, ffname FROM friendship f INNER JOIN users u ON f.fid = u.fid";

		ResultSet rs = new SelectSample().select_mysql(sqlQueryStr);
		try {
			while (rs.next()) {
				System.out.print(rs.getString("fid")+"  ::  ");
				System.out.print(rs.getString("fname")+"  ::  ");
				System.out.print(rs.getString("ffid")+"  ::  ");
				System.out.println(rs.getString("ffname"));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		// test select
//		String sqlQueryStr = "SELECT top 100 Hire_No, Cust_Code,Branch_Code,Site,hire_revenue FROM hin.vwhireMast;";
//		ResultSet rs = new SelectSample().select(sqlQueryStr);
//		try {
//			while (rs.next()) {
//				System.out.print(rs.getString("Hire_No")+"  ::  ");
//				System.out.print(rs.getString("Cust_Code")+"  ::  ");
//				System.out.print(rs.getString("Branch_Code")+"  ::  ");
//				System.out.print(rs.getString("Site")+"  ::  ");
//				System.out.println(rs.getString("hire_revenue"));
//			}
//		} catch (SQLException e) {
//			e.printStackTrace();
//		}
	}
	
	
	
	
	
	//connect sql server
	public ResultSet select(String sqlQueryStr) {
		try {
			Connection con = new SimpleConnetion().getConnection_EDW();
			PreparedStatement stm = con.prepareStatement(sqlQueryStr);
			ResultSet rs = stm.executeQuery();
			return rs;
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
	}
	
	//connect mysql 
	public ResultSet select_mysql(String sqlQueryStr) {
		try {
			Connection con = new SimpleConnetion().getConnection_MySQL();
			PreparedStatement stm = con.prepareStatement(sqlQueryStr);
			ResultSet rs = stm.executeQuery();
			return rs;
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
	}

}
