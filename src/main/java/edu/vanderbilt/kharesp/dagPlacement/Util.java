package edu.vanderbilt.kharesp.dagPlacement;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.UnknownHostException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Util {
	private static Logger logger=LogManager.getLogger(Util.class.getSimpleName());

	public static String hostName(){
		String hostname=null;
		try {
			hostname= InetAddress.getLocalHost().getHostName();
		} catch (UnknownHostException e) {
        	logger.error(e.getMessage(),e);
		}
		return hostname;
	}
	
	public static void executeCommand(String command,String outputFile){
		PrintWriter writer=null;
		try {
			writer= new PrintWriter(outputFile,"UTF-8");
			Process p = Runtime.getRuntime().exec(command);
			BufferedReader stdInp = new BufferedReader(new InputStreamReader(p.getInputStream()));
			BufferedReader stdErr = new BufferedReader(new InputStreamReader(p.getErrorStream()));
			String line;
			while ((line=stdInp.readLine()) != null) {
                writer.write(line+"\n");
            }
			while ((line=stdErr.readLine()) != null) {
				logger.error(line+"\n");
            }
			stdInp.close();
			stdErr.close();
            
		} catch (IOException e) {
			logger.error(e.getMessage(),e);
		}finally{
			writer.close();
		}
	}
}
