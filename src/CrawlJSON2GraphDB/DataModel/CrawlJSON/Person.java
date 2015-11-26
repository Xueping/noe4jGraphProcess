package CrawlJSON2GraphDB.DataModel.CrawlJSON;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
public class Person {

	private long index;	
	private String UserName;
	private String UID;
	private boolean isLike;
	private List<String> FriendList = new ArrayList<String>();
	private String ProfileLink;
	private String PhotoSrc;
	private List<Post> PostsLikeList = new ArrayList<Post>();
	private boolean infoCompleted = false; // indicate whether the information is complete
	
	/**
	 * Constructor with uncompleted parameters.
	 * @param uid
	 * @param username
	 * @param photosrc
	 * @param profilelink
	 */
	public Person(String uid, String username, String photosrc, String profilelink)
	{
		this.setUID(uid);
		this.setUserName(username);
		this.setPhotoSrc(photosrc);
		this.setProfileLink(profilelink);
		this.setInfoCompleted(false);
		this.setLike(false);
	}
	
	/**
	 * Constructor with completed parameters.
	 * Construct a person from the jsonFile, and add the person's friend to global variable -- allpersons.
	 * @param jsonFile
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	public Person(File jsonFile, AllPersons allpersons) throws FileNotFoundException, IOException
	{
		JSONParser parser=new JSONParser();
		try
		{
		    Object obj = parser.parse(new FileReader(jsonFile));
		    JSONObject jsonObject = (JSONObject) obj;
			this.setUserName((String) jsonObject.get("UserName"));
			this.setUID((String) jsonObject.get("UID"));
			this.setProfileLink((String) jsonObject.get("UprofileLink"));
			this.setPhotoSrc((String) jsonObject.get("PhotoSrc"));
			this.setLike(true);
			this.setInfoCompleted(true);
			JSONArray ja = (JSONArray) jsonObject.get("FriendList");
			for(Object object : ja)
			{
				JSONObject jo = (JSONObject) object;
				String FPhotoSrc = (String) jo.get("FPhotoSrc");
				String FUserName = (String) jo.get("FUserName");
				String FUID = (String) jo.get("FUID");
				String FUprofileLink = (String) jo.get("FUprofileLink");
				this.getFriendList().add(FUID);

				// create a new temporal person, and put it into the person list.
				// there are some persons we know them but have not crawl their profile.
				Person person = new Person(FUID, FUserName, FPhotoSrc, FUprofileLink);
				if(allpersons.addPersonWithChk(person) == 1)
				{
//					System.out.println("**add person " + FUID);
				}
				else
				{
					System.out.println("--repeat person " + FUID);
				}

			}
	    }
		catch(ParseException pe)
		{
		    System.out.println("position: " + pe.getPosition());
		    System.out.println(pe);
	    }
	}
	
	



	public String getUserName() {
		return UserName;
	}


	public void setUserName(String userName) {
		UserName = userName;
	}


	public String getUID() {
		return UID;
	}


	public void setUID(String uID) {
		UID = uID;
	}


	public List<String> getFriendList() {
		return FriendList;
	}


	public void setFriendList(List<String> friendList) {
		FriendList = friendList;
	}


	public String getProfileLink() {
		return ProfileLink;
	}


	public void setProfileLink(String profileLink) {
		ProfileLink = profileLink;
	}

	public String getPhotoSrc() {
		return PhotoSrc;
	}

	public void setPhotoSrc(String photoSrc) {
		PhotoSrc = photoSrc;
	}
	

	public List<Post> getPostsLikeList() {
		return PostsLikeList;
	}

	public void setPostsLikeList(List<Post> postsLikeList) {
		PostsLikeList = postsLikeList;
	}

	public boolean isInfoCompleted() {
		return infoCompleted;
	}

	public void setInfoCompleted(boolean infoCompleted) {
		this.infoCompleted = infoCompleted;
	}

	public long getIndex() {
		return index;
	}

	public void setIndex(long index) {
		this.index = index;
	}

	public boolean isLike() {
		return isLike;
	}

	public void setLike(boolean isLike) {
		this.isLike = isLike;
	}
}
