package datatypes.base;

import common.DatabaseConstants;

public abstract class DataType<T> {
	protected T value;
	protected boolean isNull;
	
	public String getStringValue()
	{
		if(value==null)
		{
			return "NULL";
		}
		return value.toString();
	}
	
	
	public boolean isNull()
	{
		return isNull;
	}
}
