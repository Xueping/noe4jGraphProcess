package hin.qcis.ch.transform;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

public class InputStreamToByte {
	
	static public byte[] inputStreamToByte(InputStream is) throws IOException {  
		   ByteArrayOutputStream bytestream = new ByteArrayOutputStream();  
		   int ch;  
		   while ((ch = is.read()) != -1) {  
		    bytestream.write(ch);  
		   }  
		   byte imgdata[] = bytestream.toByteArray();  
		   bytestream.close();  
		   return imgdata;  
		  } 

}
