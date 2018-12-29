package common;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.text.ParseException;
import java.io.File;

public class Utils {
	
	public static String getDatabasePath(String databaseName)
	{
		return DatabaseConstants.DEFAULT_DATA_DIRNAME+"/"+databaseName;
	}
	
	public static void printMessage(String str)
	{
		System.out.println(str);
	}
	
	public static void printMissingTableError(String databaseName,String tableName)
	{
		printMessage("Error(105T): Table '"+databaseName+"."+tableName+"'does not exist");
	}
	
	public static void printMissingDatabaseError(String databaseName)
	{
		printMessage("Error(104T): Database '"+databaseName+"'does not exist");
	}
	public static boolean isValidDateFormat(String date)
	{
		DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
		formatter.setLenient(false);
		try
		{
			formatter.parse(date);
		}
		catch(ParseException e)
		{
			return false;
		}
		return true;
		
	}
	public static boolean isValidDateTimeFormat(String date)
	{
		DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		formatter.setLenient(false);
		try
		{
			formatter.parse(date);
		}
		catch(ParseException e)
		{
			return false;
		}
		return true;
	}
	
	public static boolean RecursivelyDelete(File file)
	{
		if(file==null)
		{
			return true;
		}
		boolean isDeleted;
		if(file.isDirectory())
		{
			for(File childFile:file.listFiles())
			{
				if(childFile.isFile())
				{
				isDeleted=childFile.delete()	;
				if(!isDeleted)
				{
					return false;
				}
				}
				else
				{
					isDeleted = RecursivelyDelete(childFile);
					if(!isDeleted)
					{
						return false;
					}
					}
				}
			}
		return file.delete()
;		
	}

}
