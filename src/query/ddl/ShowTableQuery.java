package query.ddl;

import query.base.IQuery;
import query.model.result.Result;
import java.util.ArrayList;
import query.model.parser.Condition;

public class ShowTableQuery implements IQuery{
	
	public String databaseName;
	
	public ShowTableQuery(String databaseName)
	{
		this.databaseName=databaseName;
	}
	
	@Override
	public Result ExecuteQuery()
	{
		ArrayList<String> columns = new ArrayList<String>();
		columns.add("table_name");
		
		Condition condition = Condition.CreateCondition
	}

}
