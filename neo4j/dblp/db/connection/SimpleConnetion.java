package db.connection;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class SimpleConnetion {

	public SimpleConnetion() {}
	
	public Connection getConnection_MySQL() {
		try {
			
			String url = "jdbc:mysql://localhost/user_db";
			Class.forName ("com.mysql.jdbc.Driver").newInstance ();
			Connection conn = DriverManager.getConnection (url, "root", "root");
			return conn;
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		} catch (InstantiationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		} 
	}
	
	public Connection getConnection_DBLP_MySQL() {
		try {
			String url = "jdbc:mysql://qcis4/dblp";
			try {
				Class.forName ("com.mysql.jdbc.Driver").newInstance ();
			} catch (InstantiationException | IllegalAccessException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			Connection conn = DriverManager.getConnection (url, "root", "passwd");
			return conn;
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
	}
}
