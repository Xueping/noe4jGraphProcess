package CrawlJSON2GraphDB.DataModel.Gephi;

public class GMLNode {

	private Long id;
	private String label;
	
	public GMLNode(Long nodeid, String nodelab)
	{
		this.setId(nodeid);
		this.setLabel(nodelab);
	}
	
	public Long getId() {
		return id;
	}
	public void setId(Long id) {
		this.id = id;
	}
	public String getLabel() {
		return label;
	}
	public void setLabel(String label) {
		this.label = label;
	}
	
	public String toString()
	{
		String str = "  node\n  [\n    id " + getId() + "\n    " + "label \"" + getLabel() + "\"\n  ]\n";
		return str;
	}
	
	public static void main(String[] args)
	{
		GMLNode node = new GMLNode((long) 1, "hello");
		System.out.print(node.toString());
	}
}
