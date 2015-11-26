package metapath.graph;

import org.neo4j.graphdb.RelationshipType;

public class RelationshipTypeImpl implements RelationshipType{
	
	private String name;

	public RelationshipTypeImpl(String name) {
		this.name = name;
	}

	@Override
	public String name() {
		// TODO Auto-generated method stub
		return this.name;
	}

}
