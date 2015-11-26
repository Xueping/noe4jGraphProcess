package CrawlJSON2GraphDB.DataModel.Gephi;

public class GMLLink {

	private GMLNode srcNode;
	private GMLNode dstNode;
	private double value;
	
	public GMLLink()
	{
		super();
	}
	public GMLLink(GMLNode snode, GMLNode dnode, double val)
	{
		init(snode, dnode, val);
	}
	public void init(GMLNode snode, GMLNode dnode, double val)
	{
		this.setSrcNode(snode);
		this.setDstNode(dnode);
		this.setValue(val);
	}
	
	public GMLNode getSrcNode() {
		return srcNode;
	}
	public void setSrcNode(GMLNode srcNode) {
		this.srcNode = srcNode;
	}
	public GMLNode getDstNode() {
		return dstNode;
	}
	public void setDstNode(GMLNode dstNode) {
		this.dstNode = dstNode;
	}
	public double getValue() {
		return value;
	}
	public void setValue(double value) {
		this.value = value;
	}
	

	public String toString()
	{
		String str = "  edge\n  [\n    source " + getSrcNode().getId() + "\n    " + "target " + getDstNode().getId() + "\n    value "+ getValue() + "\n  ]\n";
		return str;
	}
	
	public static void main(String[] args)
	{
		GMLNode node1 = new GMLNode((long) 1, "hello");
		GMLNode node2 = new GMLNode((long) 2, "world");
		GMLLink link = new GMLLink(node1, node2, 0.8);
		System.out.print(link.toString());
	}
}
