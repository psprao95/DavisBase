package datatypes.base;

import common.DatabaseConstants;
import query.model.parser.Literal;
import datatypes.*;

public abstract class DataType<T> {
	protected T value;
	protected boolean isNull;
	protected final byte valueSerialCode;
	protected final byte nullSerialCode;
	
	public static DataType CreateDT(Literal value)
	{
		switch(value.type)
		{
		case TINYINT:
			return new DataType_TinyInt(Byte.valueOf(value.value));
			
		case SMALLINT:
			
			
		}
	}
	
	
	protected DataType(int valueSerialCode, int nullSerialCode)
	{
		valueSerialCode=(byte)valueSerialCode;
		nullSerialCode=(byte)nullSerialCode;
	}
	
	public T getValue()
	{
		return value;
	}
	
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
