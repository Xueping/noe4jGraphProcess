package hin.qcis.ch.domain;


public class Link {
	
	private long source;
	private String sourceUID;
	private long target;
	private String targetUID;
	private int value;
	private int ID;	
	
	public long getSource() {
		return source;
	}
//	@XmlAttribute
	public long getTarget() {
		return target;
	}
	
	public int getValue() {
		return value;
	}
//	@XmlAttribute
	public void setSource(long source) {
		this.source = source;
	}
	
	public void setTarget(long target) {
		this.target = target;
	}
//	@XmlAttribute
	public void setValue(int value) {
		this.value = value;
	}
	
	public int getID() {
		return ID;
	}
//	@XmlAttribute
	public void setID(int iD) {
		ID = iD;
	}
	public String getSourceUID() {
		return sourceUID;
	}
	public String getTargetUID() {
		return targetUID;
	}
	public void setSourceUID(String sourceUID) {
		this.sourceUID = sourceUID;
	}
	public void setTargetUID(String targetUID) {
		this.targetUID = targetUID;
	}
	
	
	

}
