package common;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.text.ParseException;

public class Utils {
	
	
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

}
