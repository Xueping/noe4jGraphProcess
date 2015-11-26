package CrawlJSON2GraphDB.DataModel.CrawlJSON;

public class Post {

	private String href;
	private String PostID;
	
	public Post(String url)
	{
		this.href = url;
		this.PostID = parseIDFromHref(url);
	}
	
	private String parseIDFromHref(String url)
	{
		if (url.length() < 1 && url.indexOf("/") < 0)
		{
			return "";
		}
		String key = "posts/";
		int beginIndex = url.indexOf(key) + key.length();
		String postID = url.substring(beginIndex);
		return postID;
	}
	
	public String getHref()
	{
		return href;
	}
	
	public String getPostID()
	{
		return PostID;
	}
	
}
