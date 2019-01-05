package userInterface;

import java.util.Scanner;



import query.QueryHandler;
import common.CatalogDatabaseHelper;
import common.DatabaseConstants;
import query.QueryParser;

/**
 *  created by prashanth on 27th december 2018 
 */

public class DavisBasePrompt {

	private static Scanner scanner = new Scanner(System.in).useDelimiter(";");
	
	public static void main(String args[])
	{
		CatalogDatabaseHelper.InitializeDatabase();
		splashScreen();
		while(!QueryParser.isExit)
		{
			System.out.println(DatabaseConstants.PROMPT);
			String command=scanner.next().replace("\n", "").replace("\r", " ").trim().toLowerCase();
			QueryParser.parseCommand(command);
			}
	}
	
	private static void splashScreen()
	{
		System.out.println(QueryHandler.line("-", 80));
		System.out.println("Welcome to DavisBaseListe");
		QueryHandler.ShowVersionQueryHandler();
		System.out.println("\nType 'help;' to display suppoted commands");
		System.out.println(QueryHandler.line("-", 80));
	}
}
