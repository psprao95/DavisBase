package userInterface;

import java.util.Scanner;



import query.QueryHandler;

/**
 *  created by prashanth on 27th december 2018 
 */

public class DavisBasePrompt {

	private static Scanner scanner = new Scanner(System.in).useDelimiter(";");
	
	public static void main(String args[])
	{
	
	}
	
	private static void splashScreen()
	{
		System.out.println(QueryHandler.line("-", 80));
		System.out.println("Welcome to DavisBaseListe");
	
	}
}
