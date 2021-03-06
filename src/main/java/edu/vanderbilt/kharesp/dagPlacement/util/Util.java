package edu.vanderbilt.kharesp.dagPlacement.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Util {
	public static final String CONTROL_TOPIC="DAG_PLACEMENT";
	public static final String CTRL_CMD_START_PUBLISHING="start";
	public static final String CTRL_CMD_EXIT="exit";
	public static final String CONTROL_SOCKET_CONNECTOR="tcp://129.59.105.159:5000";

	private static Logger logger=LogManager.getLogger(Util.class.getSimpleName());
	@SuppressWarnings("serial")
	public static final Map<Integer, Integer> bogusIterations=new HashMap<Integer,Integer>(){{
		put(1,1);
		put(5,6);
		put(10,12);
		put(15,18);
		put(20,24);
		put(25,30);
		put(30,36);
		put(35,42);
		put(40,48);
	}};

	public static String hostName(){
		String hostname=null;
		try {
			hostname= InetAddress.getLocalHost().getHostName();
		} catch (UnknownHostException e) {
        	logger.error(e.getMessage(),e);
		}
		return hostname;
	}
	
	public static String ipAddress(){
		String ip=null;
		try {
             ip= InetAddress.getLocalHost().getHostAddress();
        } catch (java.net.UnknownHostException e) {
        	logger.error(e.getMessage(),e);
        }
		return ip;
	}
	
	public static String ipAddressIface(String interfaceName){
		String ip = null;
		try {
			NetworkInterface networkInterface;
			networkInterface = NetworkInterface.getByName(interfaceName);
			Enumeration<InetAddress> inetAddress = networkInterface.getInetAddresses();
			InetAddress currentAddress;
			currentAddress = inetAddress.nextElement();
			while (inetAddress.hasMoreElements()) {
				currentAddress = inetAddress.nextElement();
				if (currentAddress instanceof Inet4Address && !currentAddress.isLoopbackAddress()) {
					ip = currentAddress.toString();
					break;
				}
			}
		} catch (SocketException e) {
			logger.error(e.getMessage(), e);
		}
		return ip;
	}

	public static void executeCommand(String command,String outputFile){
		logger.info("Executing command:{}",command);
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
	
	public static void bogus(int processingInterval){
		//fib(22) was benchmarked on BBB and it takes ~1ms on average 
		if (Util.bogusIterations.containsKey(processingInterval)){
			for(int i=0; i< Util.bogusIterations.get(processingInterval);i++){
				Util.fib(22);
			}
		}
	}
	
	public static int fib(int n) {
		if (n <= 1)
			return n;
		return fib(n - 1) + fib(n - 2);
	}
	
	public static void createZNode(String zkConnector,String path){
		try {
			CuratorFramework client = CuratorFrameworkFactory.newClient(zkConnector,
					new ExponentialBackoffRetry(1000, 3));
			client.start();
			client.create().forPath(path);
			client.close();
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
		}
	}
	
	public static long getTimeUSec(){
		return System.nanoTime()/1000;
	}
	
}
