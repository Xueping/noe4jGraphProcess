package commons;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;


public class GraphDBMgt_neo4j implements IGraphDBMgt{
	
	@Override
	public GraphDatabaseService startDB(String DB_PATH)
	{
		GraphDatabaseService graphdb = new GraphDatabaseFactory()
				.newEmbeddedDatabaseBuilder( DB_PATH )
				.setConfig( GraphDatabaseSettings.nodestore_mapped_memory_size, "100M" )
			    .setConfig( GraphDatabaseSettings.string_block_size, "600" )
			    .setConfig( GraphDatabaseSettings.array_block_size, "3000" )
			    .newGraphDatabase();
		registerShutdownHook( graphdb );
		
		return graphdb;
	}

	@Override
	public void stopDB(GraphDatabaseService graphDb)
	{
		if (graphDb != null)
		{
			graphDb.shutdown();
		}
	}
	
	private void registerShutdownHook(final GraphDatabaseService graphDb )
	{
	    // Registers a shutdown hook for the Neo4j instance so that it
	    // shuts down nicely when the VM exits (even if you "Ctrl-C" the
	    // running application).
	    Runtime.getRuntime().addShutdownHook( new Thread()
	    {
	        @Override
	        public void run()
	        {
	            graphDb.shutdown();
	        }
	    } );
	}


	public static enum RelTypes implements RelationshipType
	{
	    WRITE, 
	    PUBLISH
	}
	
	public static enum Labels implements Label
	{
		AUTHOR, 
	    PAPER,
	    CONFERENCE
	}
	
	public static enum Direction implements RelationshipType
	{
	    OUTGOING
	}

}
