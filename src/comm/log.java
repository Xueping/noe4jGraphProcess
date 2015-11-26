package comm;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class log {

	public static File logfile = new File(System.getProperty("user.dir") + "/log.txt");

	public static void print(String line)
	{
		FileWriter fw = null;
		try {
			fw = new FileWriter(logfile, true);
			fw.append(line);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		finally{
			if(fw != null)
			{
				try {
					fw.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		System.out.print(line);
	}
	public static void write(String line)
	{
		FileWriter fw = null;
		try {
			fw = new FileWriter(logfile, true);
			fw.append(line);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		finally{
			if(fw != null)
			{
				try {
					fw.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}
	
	public static void println(String line)
	{
		FileWriter fw = null;
		try {
			fw = new FileWriter(logfile, true);
			fw.append(line);
			fw.append("\n");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		finally{
			if(fw != null)
			{
				try {
					fw.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}

		System.out.print(line);
		System.out.print("\n");
	}

	public static void writeln(String line)
	{
		FileWriter fw = null;
		try {
			fw = new FileWriter(logfile, true);
			fw.append(line);
			fw.append("\n");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		finally{
			if(fw != null)
			{
				try {
					fw.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}
}
