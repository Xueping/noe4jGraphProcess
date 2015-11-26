package CRM;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;


public class main {

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub

		String path = "/Users/jingjiang/Google Drive/CoatesHire/DataCrawler/Contacts";
		File folder = new File(path);
		File outcome = new File(path+"all_cust.csv");
		FileWriter fw = new FileWriter(outcome);
		int countRows = 0;
		for(File file : folder.listFiles())
		{
			System.out.println(file.getName());
			
			if (file.getName().equalsIgnoreCase("all_cust.csv") == true || file.getName().indexOf(".csv") < 0)
			{
				continue;
			}
			System.out.println("Hello");
			FileReader fr = new FileReader(file);
			BufferedReader br = new BufferedReader(fr);
			String line = null;
			while((line = br.readLine()) != null)
			{
				if (line.length() > 2)
				{
					countRows++;
					fw.write(line + "\n");
				}
			}
			br.close();
			fr.close();
		}

		fw.flush();
		fw.close();
		System.out.println("countRows=" + countRows);
	}

}
