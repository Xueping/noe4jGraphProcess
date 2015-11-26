package hin.qcis.ch.graphDB;

import org.neo4j.graphdb.GraphDatabaseService;

public interface IGraphDBMgt {

	/**
	 * Start a graph database instance.
	 * @param DB_PATH - the storing folder of the graph database.
	 * @return the started Graph database object.
	 * @author Guodong Long
	 */
	public GraphDatabaseService startDB(String DB_PATH);
	
	/**
	 * Stop the graph database instance.
	 * @param startDB - the stopping graph database instance.
	 * @author Guodong Long
	 */
	public void stopDB(GraphDatabaseService startDB);
	
}
