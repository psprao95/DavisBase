package io;

import java.io.File;

import common.DatabaseConstants;
import common.Utils;

public class IOManager {
	
	public boolean databaseExists(String databaseName)
	{
		File databaseDir=new File(Utils.getDatabasePath(databaseName));
		return databaseDir.exists();
	}
	
	public boolean checkTableExists(String databaseName, String tableName)
	{
		boolean databaseExists=this.databaseExists(databaseName);
		boolean fileExists = new File(Utils.getDatabasePath(databaseName)+"/"+tableName+DatabaseConstants.DEFAULT_FILE_EXTENSION).exists();
		return (databaseExists&&fileExists);
	}

}
