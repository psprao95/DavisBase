package datatypes;

import common.DatabaseConstants;
import datatypes.base.DataType_Numeric;
public class DataType_BigInt extends DataType_Numeric<Long> {
	
	public DataType_BigInt()
	{
		this(0,true);
	}
	
	public DataType_BigInt(Long value)
	{
		this(value==null?0:value,value==null);
	}
	
	public DataType_BigInt(long value, boolean isNull)
	{
		super(DatabaseConstants.BIG_INT_SERIAL_TYPE_CODE,DatabaseConstants.EIGHT_BYTE_NULL_SERIAL_TYPE_CODE,Long.BYTES);
		this.value=value;
		this.isNull=isNull;
	}
	
	@Override
	public void increment(Long value)
	{
		this.value+=value;
	}
	
	@Override
	public boolean compare(DataType_Numeric<Long> object2, short condition)
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
	
	public boolean compare(DataType_TinyInt object2, short condition)
	{
		DataType_BigInt object=new DataType_BigInt(object2.getValue(),false);
		return this.compare(object,condition);
	}
	
	public boolean compare(DataType_Int object2, short condition)
	{
		DataType_BigInt object=new DataType_BigInt(object2.getValue(),false);
		return this.compare(object,condition);
	}
	
	public boolean compare(DataType_SmallInt object2, short condition)
	{
		DataType_BigInt object=new DataType_BigInt(object2.getValue(),false);
		return this.compare(object,condition);
	}
	
	
}
