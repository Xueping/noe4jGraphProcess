package hin.qcis.ch.domain;


public class Node {
	
	
	private String ProfilLink;
	private String UID;
	private String name;
	private int ID;
	private String PhotoSrc;
	private String UserName;
	private String isLike;
	private int group;
	
	
	public String getProfilLink() {
		return ProfilLink;
	}
	public void setProfilLink(String profilLink) {
		ProfilLink = profilLink;
	}
	
	public String getUID() {
		return UID;
	}
	public void setUID(String uID) {
		UID = uID;
	}
	
	public String getName() {
		return name;
	}
	
	public void setName(String name) {
		this.name = name;
	}
	
	public int getID() {
		return ID;
	}
	public void setID(int iD) {
		ID = iD;
	}
	
	public String getPhotoSrc() {
		return PhotoSrc;
	}
	public void setPhotoSrc(String photoSrc) {
		PhotoSrc = photoSrc;
	}
	
	public String getUserName() {
		return UserName;
	}
	public void setUserName(String userName) {
		UserName = userName;
	}
	
	public int getGroup() {
		return group;
	}
	public void setGroup(int group) {
		this.group = group;
	}
	
	public String getIsLike() {
		return isLike;
	}
	public void setIsLike(String isLike) {
		this.isLike = isLike;
	}


}
