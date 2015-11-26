package hin.qcis.ch.domain;

import java.util.ArrayList;


public class Graph {
	
	private ArrayList<Node> nodes;
	private ArrayList<Link> links;
	private Node startNode;
	private ArrayList<Node> likeNodes;
	
	
	public ArrayList<Node> getNodes() {
		return nodes;
	}

	public void setNodes(ArrayList<Node> nodes) {
		this.nodes = nodes;
	}
	
	public ArrayList<Link> getLinks() {
		return links;
	}
	public void setLinks(ArrayList<Link> links) {
		this.links = links;
	}
	
	public Node getStartNode() {
		return startNode;
	}
	public void setStartNode(Node startNode) {
		this.startNode = startNode;
	}
	
	public ArrayList<Node> getLikeNodes() {
		return likeNodes;
	}
	public void setLikeNodes(ArrayList<Node> likeNodes) {
		this.likeNodes = likeNodes;
	}
	
	public String toXML()
	{
		StringBuilder sb = new StringBuilder();
		sb.append("<graph directed=\"0\"> \n");
		
		for(Node node : nodes) {
			sb.append("	<node id=\""+node.getUID()+"\">\n");
			sb.append("		<att name=\"uid\" value=\""+node.getUID()+"\"/>\n");
			sb.append("		<att name=\"isLike\" value=\""+node.getIsLike()+"\"/>\n");
			sb.append("		<att name=\"userName\" value=\""+node.getUserName()+"\"/>\n");
			sb.append("		<att name=\"profilLink\" value=\""+node.getProfilLink()+"\"/>\n");
			sb.append("		<att name=\"photoSrc\" value=\""+node.getPhotoSrc()+"\"/>\n");
			sb.append("	 </node>\n");
			
		}

		for(Link link : links) {
			sb.append("	<edge source=\""+link.getSourceUID()+"\" target=\""+link.getTargetUID()+"\">  </edge>\n");
		}
		sb.append("</graph>");

//		return sb.toString().replace("/", "\\");
		return sb.toString();
	}


}
