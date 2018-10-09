package edu.vanderbilt.kharesp.dagPlacement;

import java.nio.file.Paths;

public class Test {
	public static void main(String args[]){
		System.out.println(Paths.get(".").toAbsolutePath().normalize().toString());
	}

}
