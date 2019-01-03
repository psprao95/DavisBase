package query.ddl;

import query.base.IQuery;
import java.util.ArrayList;
import query.model.parser.Column;
import query.model.result.Result;
public class CreateTableQuery implements IQuery{
	public String tableName;
	public ArrayList<Column> columns;
	private boolean hasPrimary;
	public String databaseName;
	
	public CreateTableQuery(String databaseName,String tableName, ArrayList<Column> columns, boolean hasPrimary)
	{
		this.tableName=tableName;
		this.columns=columns;
		this.hasPrimary=hasPrimary;
		this.databaseName=databaseName;
	}
	
	@Override
	public Result ExecuteQuery()
	{
		return new Result(1);
	}
	

}
