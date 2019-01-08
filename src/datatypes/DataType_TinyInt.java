package datatypes;

import common.DatabaseConstants;
import datatypes.base.DataType_Numeric;
public class DataType_TinyInt extends DataType_Numeric<Byte>{
	
	public DataType_TinyInt()
	{
		this((byte)0,true);
	}
	
	public DataType_TinyInt(Byte value)
	{
		this(value==null?0:value,value==null);
	}
	
	public DataType_TinyInt(byte value,boolean isNull)
	{
		super(DatabaseConstants.TINY_INT_SERIAL_TYPE_CODE,DatabaseConstants.ONE_BYTE_NULL_SERIAL_TYPE_CODE,Byte.BYTES);
	}
	
	@Override
	public void increment(Byte value)
	{
		this.value=(byte)(this.value+value);
	}
	
	@Override 
	public boolean compare(DataType_Numeric<Byte> object2,short condition)
	{
		if(value==null)
		{
			return false;
		}
		switch(condition)
		{
		case DataType_Numeric.EQUALS:
			return value==object2.getValue();
			
		case DataType_Numeric.GREATER_THAN:
			return value>object2.getValue();
			
		case DataType_Numeric.LESS_THAN:
			return value<object2.getValue();
			
		case DataType_Numeric.GREATER_THAN_EQUALS:
			return value>=object2.getValue();
			
		case DataType_Numeric.LESS_THAN_EQUALS:
			return value<=object2.getValue();
			
			default:
				return false;
		}
	}
}
