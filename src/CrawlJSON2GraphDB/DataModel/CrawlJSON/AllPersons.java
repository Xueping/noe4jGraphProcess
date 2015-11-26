package CrawlJSON2GraphDB.DataModel.CrawlJSON;

import java.util.HashMap;

import comm.log;

public class AllPersons {
	
	private HashMap<String, Person> all = new HashMap<String, Person>();
	
	/**
	 * Add a person into the person list;
	 * @param person
	 */
	public void addPerson(Person person)
	{
		if(person.getUID() == null)
		{
			log.print("add a null person");
			return;
		}
		all.put(person.getUID(), person);
	}
	
	/**
	 * Check whether the person existing before adding the person. 
	 * If existing, return 0;  --represent 0 person is added.
	 * Else return 1;          --represent 1 person is added.
	 * @param person
	 * @return
	 */
	public int addPersonWithChk(Person newPerson)
	{
		String UID = newPerson.getUID();
		if (all.containsKey(UID))
		{
			Person existPerson = all.get(UID);
			if(!existPerson.isInfoCompleted() && newPerson.isInfoCompleted())
			{
				System.out.println("replace person" + newPerson.getUID());
				addPerson(newPerson);
			}
			return 0;
		}
		else
		{
			addPerson(newPerson);
			return 1;
		}
	}
	
	public HashMap<String, Person> getHashMap()
	{
		return all;
	}
	
	/**
	 * Search person by UID
	 * @param UID
	 * @return
	 */
	public Person queryPersonByUID(String UID)
	{
		if (all.containsKey(UID))
		{
			return all.get(UID);
		}
		else
		{
			return null;
		}
	}
	
	public int getPersonCount()
	{
		return all.size();
	}
	
}
