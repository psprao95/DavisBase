package query.vdl;

import query.base.*;
public class DescTableQuery implements IQuery{
	String tableName;
	String databaseName;
	
	public DescTableQuery(String databaseName,String tableName)
	{
		this.tableName=tableName;
		this.databaseName=databaseName;
	}

}
