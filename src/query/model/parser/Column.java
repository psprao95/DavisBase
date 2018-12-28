package query.model.parser;

import query.QueryHandler;
public class Column {
	public String name;
	public boolean isNull;
	public  DataTypeEnum type;
	
	public static Column createColumn(String columnString)
	{
		String primaryKeyString="primary key";
		String notNullString="not null";
		boolean isNull=true;
		if(columnString.toLowerCase().endsWith(primaryKeyString))
		{
			columnString = columnString.substring(0, columnString.length()-primaryKeyString.length()).trim();
		}
		else if(columnString.toLowerCase().endsWith(notNullString))
		{
			columnString=columnString.substring(0, columnString.length()-notNullString.length()).trim();
			isNull=false;
		}
		String[] parts=columnString.split(" ");
		String name;
		if(parts.length>2)
		{
			QueryHandler.UnrecognisedCommand(columnString, "Expected Column format <name> <datatype> [primary key | not null]");
			return null;
			
		}
		if(parts.length>1)
		{
			name=parts[0].trim();
			DataTypeEnum type = GetDataType(parts[1].trim());
			if(type==null)
			{
				QueryHandler.UnrecognisedCommand(columnString, "Unrecognised data type: "+parts[1]);
			}
			return new Column(name,type,isNull);
			
		}
		QueryHandler.UnrecognisedCommand(columnString, "Expected Column format <name> <datatype> [primary key | not null]");
		return null;
		
	}
	
	private Column(String name, DataTypeEnum type, boolean isNull)
	
	{
		this.name=name;
		this.type=type;
		this.isNull=isNull;
	}

	private static DataTypeEnum GetDataType(String dataTypeString)
	{
		switch(dataTypeString)
		{
		case "tinyint":
			return DataTypeEnum.TINYINT;
		case "smallint":
			return DataTypeEnum.SMALLINT;
		case "int":
			return DataTypeEnum.INT;
		case "bigint":
			return DataTypeEnum.BIGINT;
		case "real":
			return DataTypeEnum.REAL;
		case "double":
			return DataTypeEnum.DOUBLE;
		case "datetime":
			return DataTypeEnum.DATETIME;
		case "date":
			return DataTypeEnum.DATE;
		case "text":
			return DataTypeEnum.TEXT;
		}
		return null;
	}
}
