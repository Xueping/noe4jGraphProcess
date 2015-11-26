package CrawlJSON2GraphDB.DataModel.CrawlJSON;


import static org.junit.Assert.*;
import CrawlJSON2GraphDB.DataModel.CrawlJSON.Post;

import org.junit.Test;

public class PostTC {

	@Test
	public void testURL() 
	{
		String url = "https://www.facebook.com/commonwealthbank/posts/824293484250633";
		Post p = new Post(url);
		assertEquals(p.getHref(), url);		
	}

	@Test
	public void testPostID() 
	{
		String url = "https://www.facebook.com/commonwealthbank/posts/824293484250633";
		Post p = new Post(url);
		assertEquals(p.getPostID(), "824293484250633");		
	}

}
