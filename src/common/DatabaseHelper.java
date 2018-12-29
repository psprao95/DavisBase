package common;

import io.IOManager;
import query.QueryHandler;

public class DatabaseHelper {
	private static DatabaseHelper databaseHelper=null;
	public static DatabaseHelper getDatabaseHelper()
	{
		if (databaseHelper==null)
		{
			return new DatabaseHelper();
		}
		return databaseHelper;

	}
	
	private IOManager manager;
	
	private DatabaseHelper()
	{
		manager=new IOManager();
	}
	
	public boolean databaseExists(String databaseName)
	{
		if(databaseName==null || databaseName.length()==0)
		{
			QueryHandler.UnrecognisedCommand("", QueryHandler.USE_HELP_MESSAGE);
			return false;
		}
		return new IOManager().databaseExists(databaseName);
	}
	
	public boolean tableExists(String databaseName, String tableName)
	{
		if(tableName==null || databaseName==null || tableName.length()==0 || databaseName.length()==0)
		{
			QueryHandler.UnrecognisedCommand("", QueryHandler.USE_HELP_MESSAGE);
			return false;
		}
		return new IOManager().checkTableExists(databaseName,tableName);
	}

}
