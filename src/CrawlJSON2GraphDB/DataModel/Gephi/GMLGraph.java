package CrawlJSON2GraphDB.DataModel.Gephi;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

public class GMLGraph {

	private Long maxNodeId = (long) 0;
	private HashMap<Long, GMLNode> hsNodes = new HashMap<Long, GMLNode>();
	private HashMap<String, GMLLink> hsLinks = new HashMap<String, GMLLink>();
	
	public String toString()
	{
		StringBuilder sb = new StringBuilder();
		sb.append("Creator \"Graph FB CWB (#nodes:" + hsNodes.size() + " ; #links:" + hsLinks.size() + " )\" graph \n[\n");
		for(Entry<Long, GMLNode> entry : hsNodes.entrySet())
		{
			GMLNode node = entry.getValue();
			sb.append(node.toString());
		}
		for(Entry<String, GMLLink> entry : hsLinks.entrySet())
		{
			GMLLink link = entry.getValue();
			sb.append(link.toString());
		}
		sb.append("]");
		return sb.toString();
	}
	
	public void addNode(String value)
	{
		GMLNode node = new GMLNode(maxNodeId, value);
		hsNodes.put(maxNodeId, node);
		maxNodeId++;
	}
	
	public void addUndirectedLink(Long id1, Long id2, double value)
	{
		long sid, did;
		if (id1 < id2)
		{
			sid = id1;
			did = id2;
		}
		else
		{
			sid = id2;
			did = id1;
		}
		String linkIdx = sid + "-" + did;
		GMLNode snode = hsNodes.get(sid);
		GMLNode dnode = hsNodes.get(did);
		
		if (hsLinks.get(linkIdx) == null)
		{
			GMLLink link = new GMLLink(snode, dnode, value);
			hsLinks.put(linkIdx, link);
		}
	}
	
}
