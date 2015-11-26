package CrawlJSON2GraphDB.DataModel.Gephi;

public class GMLUndirectedLink extends GMLLink{
	

	public GMLUndirectedLink(GMLNode node1, GMLNode node2, double val)
	{
		GMLNode snode, dnode;
		// set srcNode as the node having smaller ID.
		if (node1.getId() < node2.getId())
		{
			snode = node1;
			dnode = node2;
		}
		else
		{
			snode = node2;
			dnode = node1;
		}
		init(snode, dnode, val);
	}

}
