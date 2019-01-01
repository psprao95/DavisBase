package query.dml;

import query.base.IQuery;
import query.model.parser.*;

public class UpdateQuery implements IQuery{
	
	public String databaseName;
	public String tableName;
	private String columnName;
	public Literal value;
	public Condition condition;
	
	public UpdateQuery(String databaseName, String tableName, String columnName, Literal value, Condition condition)
	{
		this.databaseName=databaseName;
		this.tableName=tableName;
		this.columnName=columnName;
		this.value=value;
		this.condition=condition;
	}
	

}
