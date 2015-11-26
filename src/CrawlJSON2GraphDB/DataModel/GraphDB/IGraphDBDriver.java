package CrawlJSON2GraphDB.DataModel.GraphDB;

import java.util.HashMap;

import org.neo4j.graphdb.GraphDatabaseService;

import CrawlJSON2GraphDB.Main;
import CrawlJSON2GraphDB.DataModel.CrawlJSON.Person;

public interface IGraphDBDriver {

	/**
	 * Start a graph database instance.
	 * @param DB_PATH - the storing folder of the graph database.
	 * @return the started Graph database object.
	 * @author Guodong Long
	 */
	public GraphDatabaseService startDB(String DB_PATH);
	
	/**
	 * Stop the graph database instance.
	 * @param startDB - the stopping graph database instance.
	 * @author Guodong Long
	 */
	public void stopDB(GraphDatabaseService startDB);
	
	/**
	 * query a egoNetwork (a subgraph with a center node). <br>
	 *********************************Example code********************************************************* <br>
	 *&nbsp;&nbsp;&nbsp;&nbsp;   IGraphDBDriver gdbdriver = new GraphDBDriver_neo4j(); <br>
	 *&nbsp;&nbsp;&nbsp;&nbsp;   GraphDatabaseService graphDBSvc = gdbdriver.startDB("....../data/GraphDB2/"); <br>
	 *&nbsp;&nbsp;&nbsp;&nbsp;	 String strUsers = gdbdriver.queryEgoNetwork(graphDBSvc, "UID", "sayed.k.hamidi"); <br>
	 *&nbsp;&nbsp;&nbsp;&nbsp;   gdbdriver.stopDB(graphDBSvc); <br>
	 *	 ******************************Result JSON**********************************************************  <br>
	 *{<br>
	 *	"startNode":"sayed.k.hamidi",<br>
	 *	"maxdegree":2,<br>
	 *	"nodes":<br>
	 *&nbsp;&nbsp;	[<br>
	 *&nbsp;&nbsp;	{"UID":"sayed.k.hamidi","UserName":"sayed.k.hamidi","PhotoSrc":"https:\\fbcdn-profile-a.akamaihd.net\hprofile-ak-xap1\t1.0-1\p160x160\10341420_10204125247152229_2322546791114732900_n.jpg","ProfilLink":"https:\\www.facebook.com\sayed.k.hamidi"},<br>
	 *&nbsp;&nbsp;	{"UID":"shamsullah.howaida","UserName":"shamsullah.howaida","PhotoSrc":"https:\\scontent-a-sea.xx.fbcdn.net\hprofile-xap1\l\t1.0-1\c0.0.100.100\p100x100\10500540_1436660566608451_5755208113711662577_n.jpg","ProfilLink":"https:\\www.facebook.com\shamsullah.howaida?fref=pb&hc_location=friends_tab"},<br>
	 *&nbsp;&nbsp;	{"UID":"profile.php?id=100008271965034","UserName":"profile.php?id=100008271965034","PhotoSrc":"https:\\fbcdn-profile-a.akamaihd.net\hprofile-ak-xpf1\v\t1.0-1\p100x100\10369995_1415475648738154_457192638534186542_n.jpg?oh=fe464ca4e145152ffbe961ec3edf6773&oe=54658034&__gda__=1416008146_ad7e0010feb4cdb99f51e8a2a9873feb","ProfilLink":"https:\\www.facebook.com\profile.php?id=100008271965034&fref=pb&hc_location=friends_tab"}<br>
	 *	],<br>
	 *	"links":[<br>
	 *&nbsp;&nbsp;	{"source":147,"target":0,"value":3},<br>
	 *&nbsp;&nbsp;	{"source":73,"target":0,"value":8},<br>
	 *&nbsp;&nbsp;	{"source":178,"target":0,"value":7}<br>
	 *	]<br>
	 *	"nodesnum":186,<br>
	 *	"edgesnum":186<br>
	 *}<br>
	 ***************************************************************************************************  <br>
	 * @param graphDBSvc - the running instance of graph database
	 * @param propName - the property name of the start node
	 * @param propValue - the property value of the start node
	 * @return A JSON format string containing the queried subgraph.
	 * @author Guodong Long
	 */
	public String queryEgoNetwork(GraphDatabaseService graphDBSvc, String propName, String propValue);

	/**
	 * query a egoNetwork (a subgraph with a center node). <br>
	 **************************** Example code********************************************************** <br>
	 *&nbsp;&nbsp;&nbsp;&nbsp;   IGraphDBDriver gdbdriver = new GraphDBDriver_neo4j(); <br>
	 *&nbsp;&nbsp;&nbsp;&nbsp;   GraphDatabaseService graphDBSvc = gdbdriver.startDB("....../data/GraphDB2/"); <br>
	 *&nbsp;&nbsp;&nbsp;&nbsp;	 String strUsers = gdbdriver.queryEgoNetwork(graphDBSvc, "UID", "sayed.k.hamidi", 2); <br>
	 *&nbsp;&nbsp;&nbsp;&nbsp;   gdbdriver.stopDB(graphDBSvc); <br>
	 ******************************Result JSON**********************************************************  <br>
	 *{<br>
	 *	"startNode":"sayed.k.hamidi",<br>
	 *	"maxdegree":2,<br>
	 *	"nodes":<br>
	 *&nbsp;&nbsp;	[<br>
	 *&nbsp;&nbsp;	{"UID":"sayed.k.hamidi","UserName":"sayed.k.hamidi","PhotoSrc":"https:\\fbcdn-profile-a.akamaihd.net\hprofile-ak-xap1\t1.0-1\p160x160\10341420_10204125247152229_2322546791114732900_n.jpg","ProfilLink":"https:\\www.facebook.com\sayed.k.hamidi"},<br>
	 *&nbsp;&nbsp;	{"UID":"shamsullah.howaida","UserName":"shamsullah.howaida","PhotoSrc":"https:\\scontent-a-sea.xx.fbcdn.net\hprofile-xap1\l\t1.0-1\c0.0.100.100\p100x100\10500540_1436660566608451_5755208113711662577_n.jpg","ProfilLink":"https:\\www.facebook.com\shamsullah.howaida?fref=pb&hc_location=friends_tab"},<br>
	 *&nbsp;&nbsp;	{"UID":"profile.php?id=100008271965034","UserName":"profile.php?id=100008271965034","PhotoSrc":"https:\\fbcdn-profile-a.akamaihd.net\hprofile-ak-xpf1\v\t1.0-1\p100x100\10369995_1415475648738154_457192638534186542_n.jpg?oh=fe464ca4e145152ffbe961ec3edf6773&oe=54658034&__gda__=1416008146_ad7e0010feb4cdb99f51e8a2a9873feb","ProfilLink":"https:\\www.facebook.com\profile.php?id=100008271965034&fref=pb&hc_location=friends_tab"}<br>
	 *	],<br>
	 *	"links":[<br>
	 *&nbsp;&nbsp;	{"source":147,"target":0,"value":3},<br>
	 *&nbsp;&nbsp;	{"source":73,"target":0,"value":8},<br>
	 *&nbsp;&nbsp;	{"source":178,"target":0,"value":7}<br>
	 *	]<br>
	 *	"nodesnum":186,<br>
	 *	"edgesnum":186<br>
	 *}<br>
	 ***********************************************************************************************  <br>
	 * @param graphDBSvc - the running instance of graph database
	 * @param propName - the property name of the start node
	 * @param propValue - the property value of the start node
	 * @param MAX_DEGREE - the maximal degree of friends
	 * @return A JSON format string containing the queried subgraph
	 * @author Guodong Long
	 */
	public String queryEgoNetwork(GraphDatabaseService graphDBSvc, String propName, String propValue, int MAX_DEGREE);

	/**
	 * Query users by property.: <br>
	 **************************** Example code********************************************************** <br>
	 *&nbsp;&nbsp;&nbsp;&nbsp;   IGraphDBDriver gdbdriver = new GraphDBDriver_neo4j(); <br>
	 *&nbsp;&nbsp;&nbsp;&nbsp;   GraphDatabaseService graphDBSvc = gdbdriver.startDB("....../data/GraphDB2/"); <br>
	 *&nbsp;&nbsp;&nbsp;&nbsp;	 String strUsers = gdbdriver.queryUsers(graphDBSvc, "UID", "sayed"); <br>
	 *&nbsp;&nbsp;&nbsp;&nbsp;   gdbdriver.stopDB(graphDBSvc); <br>
	 *****************************Result JSON*********************************************************  <br>
	 *{<br>
	 *"nodes":[<br>
	 *&nbsp;&nbsp;	{"UID":"sayedjavad.sarvari","UserName":"sayedjavad.sarvari","PhotoSrc":"https:\\fbcdn-profile-a.akamaihd.net\hprofile-ak-xfa1\t1.0-1\c37.37.466.466\s100x100\300098_173092319431634_507881879_n.jpg","ProfilLink":"https:\\www.facebook.com\sayedjavad.sarvari?fref=pb&hc_location=friends_tab"},<br>
	 *&nbsp;&nbsp;	{"UID":"sayed.a.hussaini","UserName":"sayed.a.hussaini","PhotoSrc":"https:\\fbcdn-profile-a.akamaihd.net\hprofile-ak-xpf1\t1.0-1\p100x100\1601447_578104388954520_6009131496853629643_n.jpg","ProfilLink":"https:\\www.facebook.com\sayed.a.hussaini?fref=pb&hc_location=friends_tab"},<br>
	 *&nbsp;&nbsp;	{"UID":"sayedali.khan1","UserName":"sayedali.khan1","PhotoSrc":"https:\\fbcdn-profile-a.akamaihd.net\hprofile-ak-prn2\t1.0-1\c170.50.621.621\s100x100\542234_235883196510305_83480662_n.jpg","ProfilLink":"https:\\www.facebook.com\sayedali.khan1?fref=pb&hc_location=friends_tab"},<br>
	 *&nbsp;&nbsp;	{"UID":"ata.sayedataullah","UserName":"ata.sayedataullah","PhotoSrc":"https:\\fbcdn-profile-a.akamaihd.net\hprofile-ak-xap1\t1.0-1\p100x100\10525978_323336087847572_9050911040114773451_n.jpg","ProfilLink":"https:\\www.facebook.com\ata.sayedataullah?fref=pb&hc_location=friends_tab"},<br>
	 *&nbsp;&nbsp;	{"UID":"sayed.jawad.520","UserName":"sayed.jawad.520","PhotoSrc":"https:\\fbcdn-profile-a.akamaihd.net\hprofile-ak-xpf1\t1.0-1\c11.0.100.100\p100x100\10386276_294415670743283_466391478957078007_n.jpg","ProfilLink":"https:\\www.facebook.com\sayed.jawad.520?fref=pb&hc_location=friends_tab"},<br>
	 *&nbsp;&nbsp;	{"UID":"sayed.h.alavi","UserName":"sayed.h.alavi","PhotoSrc":"https:\\fbcdn-profile-a.akamaihd.net\hprofile-ak-frc3\t1.0-1\c0.0.100.100\p100x100\994649_10200891982006765_6970309_n.jpg","ProfilLink":"https:\\www.facebook.com\sayed.h.alavi?fref=pb&hc_location=friends_tab"},<br>
	 *&nbsp;&nbsp;	{"UID":"sayed.k.hamidi","UserName":"sayed.k.hamidi","PhotoSrc":"https:\\fbcdn-profile-a.akamaihd.net\hprofile-ak-xap1\t1.0-1\p160x160\10341420_10204125247152229_2322546791114732900_n.jpg","ProfilLink":"https:\\www.facebook.com\sayed.k.hamidi"},<br>
	 *&nbsp;&nbsp;	{"UID":"sayedtaqi.shah.9","UserName":"sayedtaqi.shah.9","PhotoSrc":"https:\\fbcdn-profile-a.akamaihd.net\hprofile-ak-xpa1\t1.0-1\p100x100\10440821_322426951256845_4731352791493937473_n.jpg","ProfilLink":"https:\\www.facebook.com\sayedtaqi.shah.9?fref=pb&hc_location=friends_tab"},<br>
	 *  ],<br>
	 *  "links":[<br>
	 *  ]<br>
	 *}<br>
	 ***********************************************************************************************  <br>
	 * @param graphDBSvc
	 * @param propName
	 * @param propValue
	 * @param MAX_DEGREE
	 * @return A JSON format string containing the queried user list
	 */
	public String queryUsers(GraphDatabaseService graphDBSvc, String propName, String propValue);
	
	
//	public void createDatabase(GraphDatabaseService graphDBSvc, HashMap<String, Person> allpersons);
	
}
