package hin.qcis.ch.analysis;

import hin.qcis.ch.graphDB.GraphDBMgt_neo4j;
import hin.qcis.ch.graphDB.GraphDBMgt_neo4j.Labels;
import hin.qcis.ch.graphDB.GraphDBMgt_neo4j.RelTypes;
import hin.qcis.ch.graphDB.IGraphDBMgt;

import java.util.ArrayList;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.traversal.Evaluation;
import org.neo4j.graphdb.traversal.Evaluator;
import org.neo4j.graphdb.traversal.Paths;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.graphdb.traversal.Traverser;
import org.neo4j.graphdb.traversal.Uniqueness;



public class Meta_Path_Search_GraphDB{
	
	public void metaPath_Traversal(GraphDatabaseService graphDBSvc, Node startNode, ArrayList<RelationshipType> metaPath) {
		final ArrayList<RelationshipType> orderedPathContext = metaPath;
		TraversalDescription td = graphDBSvc.traversalDescription()
		        .evaluator( new Evaluator()
		        {
		            @Override
		            public Evaluation evaluate( final Path path )
		            {
		                if ( path.length() == 0 )
		                {
		                    return Evaluation.EXCLUDE_AND_CONTINUE;
		                }
		                RelationshipType expectedType = orderedPathContext.get( path.length() - 1 );
		                boolean isExpectedType = path.lastRelationship()
		                        .isType( expectedType );
		                boolean included = path.length() == orderedPathContext.size() && isExpectedType;
		                boolean continued = path.length() < orderedPathContext.size() && isExpectedType;
		                return Evaluation.of( included, continued );
		            }
		        } )
		        .uniqueness( Uniqueness.NODE_PATH );
		
		Traverser traverser = td.traverse( startNode );
		PathPrinter pathPrinter = new PathPrinter();
		String output = "";
		for ( Path path : traverser )
		{
			output = Paths.pathToString( path, pathPrinter );
		    System.out.println(output);
		}
	}
	
	
	static class PathPrinter implements Paths.PathDescriptor<Path>
	{
	    

	    @Override
	    public String nodeRepresentation( Path path, Node node )
	    {
	    	String nodeRep = "";
	    	if(node.hasLabel(Labels.CUSTOMER))
	    		nodeRep =  "(CUSTOMER:" + node.getProperty( "CUSTOMER_CODE", "" ) + ")";
	    	else if(node.hasLabel(Labels.HIREMASTER))
	    		nodeRep =  "(HireMaster:" + node.getProperty( "Hire_No", "" ) + ")";
	    	else if(node.hasLabel(Labels.BRANCH))
	    		nodeRep =  "(Branch:" + node.getProperty( "Branch_Code", "" ) + ")";
	    	else if(node.hasLabel(Labels.FLEET))
	    		nodeRep =  "(Fleet:" + node.getProperty( "Fleet_No", "" ) + ")";
	    	else if(node.hasLabel(Labels.HIREDETAIL))
	    		nodeRep =  "(Hire_No:" + node.getProperty( "Hire_No", "" ) +" Line_No:" + node.getProperty( "Line_No", "" ) +")";
	    	else if(node.hasLabel(Labels.CATEGORY))
	    		nodeRep =  "(Category:" + node.getProperty( "Fleet_Model_Code", "" ) + ")";
	    	return nodeRep;
	    }

	    @Override
	    public String relationshipRepresentation( Path path, Node from, Relationship relationship )
	    {
	        String prefix = "--", suffix = "--";
	        if ( from.equals( relationship.getEndNode() ) )
	        {
	            prefix = "<--";
	        }
	        else
	        {
	            suffix = "-->";
	        }
	        return prefix + "[" + relationship.getType().name() + "]" + suffix;
	    }
	}
	
	public static void main(String[] args){
		
		String DB_PATH = "/Users/xuepingpeng/Dropbox/projects/HIN/HIN_GDB_Profit_3";
		IGraphDBMgt graphDriver = new GraphDBMgt_neo4j();
		GraphDatabaseService graphDBSvc = graphDriver.startDB(DB_PATH);
		ArrayList<RelationshipType> metaPath = new ArrayList<RelationshipType>();
		metaPath.add( RelTypes.HIRE );
//		metaPath.add( RelTypes.FROM );
		metaPath.add( RelTypes.CONTAIN );
//		metaPath.add( RelTypes.IS );
		
		try ( Transaction tx = graphDBSvc.beginTx() )
		{
			Meta_Path_Search_GraphDB mp = new Meta_Path_Search_GraphDB();			
			for(Node node : graphDBSvc.findNodesByLabelAndProperty(Labels.CUSTOMER, "CUSTOMER_CODE", "ALLW8019"))
			{
				mp.metaPath_Traversal(graphDBSvc, node, metaPath);
			}			
		    tx.success();		    
		}
		graphDriver.stopDB(graphDBSvc);
		
	}

}