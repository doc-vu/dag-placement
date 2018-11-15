package edu.vanderbilt.kharesp.dagPlacement.util;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Sysstat {
	public static void collectStats(String logDir, long startTs, long endTs) throws IOException{
		//create logDir if it does not exist
		Path path = Paths.get(logDir);
		if (!Files.exists(path)) {
			Files.createDirectory(path);
		}

		SimpleDateFormat formatter = new SimpleDateFormat("HH:mm:ss");
		String startTime = formatter.format(new Date(startTs));
		String endTime = formatter.format(new Date(endTs));

		// collect system utilization metrics
		String utilStatsFile = String.format("%s/util_%s.csv", logDir, Util.hostName());
		String command = String.format("sadf -s %s -e %s -U -h -d -- -ur", startTime, endTime);
		Util.executeCommand(command, utilStatsFile);

		// collect network utilization metrics
		String nwStatsFile = String.format("%s/nw_%s.csv", logDir, Util.hostName());
		command = String.format("sadf -s %s -e %s -U -h -d -- -n DEV", startTime, endTime);
		Util.executeCommand(command, nwStatsFile);
	}

	public static void main(String args[]){
		if(args.length!=3){
			System.out.println("Sysstat logDir startTs endTs");
			return;
		}
		String logDir=args[0];
		long startTs=Long.parseLong(args[1]);
		long endTs=Long.parseLong(args[2]);
	
		try{
			collectStats(logDir,startTs,endTs);
		}catch(IOException e){
			e.printStackTrace();
		}
	}

}
