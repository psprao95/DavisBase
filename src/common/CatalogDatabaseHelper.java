package common;

import java.io.File;
impo

public class CatalogDatabaseHelper {
	
	public static void InitializeDatabase()
	{
		File baseDir = new File(DatabaseConstants.DEFAULT_DATA_DIRNAME);
		if(!baseDir.exists())
		{
			File catalogDir = new File(DatabaseConstants.DEFAULT_DATA_DIRNAME+"/"+DatabaseConstants.DEFAULT_CATALOG_DATABASENAME);
		}
	}

}
