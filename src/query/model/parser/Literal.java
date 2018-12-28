package query.model.parser;

import common.DatabaseConstants;
import common.Utils;
import query.QueryHandler;



public class Literal {

	public DataTypeEnum type;
	public String value;
	
	public static Literal CreateLiteral(datatypes.base.DataType value,Byte type)
	{
		if(type==DatabaseConstants.INVALID_CLASS)
		{
			return null;
		}
		else if(value.isNull())
		{
			return new Literal(DataTypeEnum.DOUBLE_DATETIME_NULL , value.getStringValue());
		}
		
		switch(type)
		{
		case DatabaseConstants.TINYINT:
			return new Literal(DataTypeEnum.TINYINT,value.getStringValue());
		case DatabaseConstants.SMALLINT:
			return new Literal(DataTypeEnum.SMALLINT,value.getStringValue());
		case DatabaseConstants.INT:
			return new Literal(DataTypeEnum.INT,value.getStringValue());
			
		case DatabaseConstants.BIGINT:
			return new Literal(DataTypeEnum.BIGINT,value.getStringValue());
			
		case DatabaseConstants.REAL:
			return new Literal(DataTypeEnum.REAL,value.getStringValue());
			
		case DatabaseConstants.DOUBLE:
			return new Literal(DataTypeEnum.DOUBLE,value.getStringValue());
		case DatabaseConstants.DATETIME:
			return new Literal(DataTypeEnum.DATETIME,value.getStringValue());
		case DatabaseConstants.DATE:
			return new Literal(DataTypeEnum.DATE,value.getStringValue());
		case DatabaseConstants.TEXT:
			return new Literal(DataTypeEnum.TEXT,value.getStringValue());
		}
		return null;
	}
	
	public static Literal CreateLiteral(String literalString)
	{
		if(literalString.startsWith("'")&& literalString.endsWith("'"))
		{
			literalString=literalString.substring(1, literalString.length()-1);
			
			if(Utils.isValidDateTimeFormat(literalString))
			{
				return new Literal(DataTypeEnum.DATETIME,literalString);
			}
			
			if(Utils.isValidDateFormat(literalString))
			{
				return new Literal(DataTypeEnum.DATE,literalString);
			}
			return new Literal(DataTypeEnum.TEXT,literalString);
		}
		
		if(literalString.startsWith("\"") && literalString.endsWith("\""))
		{
			literalString=literalString.substring(1, literalString.length()-1);
			
			if(Utils.isValidDateTimeFormat(literalString))
			{
				return new Literal(DataTypeEnum.DATETIME,literalString);
			}
			
			if(Utils.isValidDateFormat(literalString))
			{
				return new Literal(DataTypeEnum.DATE,literalString);
			}
			return new Literal(DataTypeEnum.TEXT,literalString);
		}
		
		try
		{
			Integer.parseInt(literalString);
			return new Literal(DataTypeEnum.INT,literalString);
		}
		catch(Exception e)
		{}
		
		try
		{
			Long.parseLong(literalString);
			return new Literal(DataTypeEnum.BIGINT,literalString);
		}
		catch(Exception e)
		{}
		
		try
		{
			Float.parseFloat(literalString);
			return new Literal(DataTypeEnum.REAL,literalString);
		}
		catch(Exception e)
		{}
		
		QueryHandler.UnrecognisedCommand(literalString, "Unrecognised literal found. Please use integers,strings or reals");
		return null;
	}
	
	
	private Literal(DataTypeEnum type, String value)
	{
		this.type=type;
		this.value=value;
	}
	
	@Override
	public String toString()
	{
		if(this.type==DataTypeEnum.TEXT)
		{
			return this.value;
		}
		else if(this.type==DataTypeEnum.INT || this.type==DataTypeEnum.SMALLINT || this.type==DataTypeEnum.SMALLINT
				|| this.type==DataTypeEnum.BIGINT)
		{
			return this.value;
		}
		else if(this.type==DataTypeEnum.DOUBLE || this.type==DataTypeEnum.REAL)
		{
			return String.format("%.2f",Double.parseDouble(this.value));
		}
		else if(this.type==DataTypeEnum.SMALL_INT_NULL || this.type==DataTypeEnum.TINY_INT_NULL || this.type==DataTypeEnum.INT_REAL_NULL
				|| this.type==DataTypeEnum.DOUBLE_DATETIME_NULL)
		{
			return "NULL";
		}
		else if(this.type==DataTypeEnum.DATETIME || this.type==DataTypeEnum.DATE)
		{
			return this.value;
		}
		return "";
	}
}
