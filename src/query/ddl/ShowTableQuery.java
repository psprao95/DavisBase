package query.ddl;

import query.base.IQuery;
import query.model.result.Result;
import java.util.ArrayList;
import query.model.parser.Condition;
import common.DatabaseConstants;
import common.DatabaseHelper;
import query.vdl.*;
import common.Utils;

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
		
		Condition condition = Condition.CreateCondition(String.format("databasename='%s'", this.databaseName));
		ArrayList<Condition> conditionList = new ArrayList<>();
		conditionList.add(condition);
		
		IQuery query = new SelectQuery(DatabaseConstants.DEFAULT_CATALOG_DATABASENAME, DatabaseConstants.SYSTEM_TABLES_TABLENAME,columns,conditionList,false);
		if(query.ValidateQuery())
		{
			query.ExecuteQuery();
		}
		return null;
	}
	
	@Override
	public boolean ValidateQuery()
	{
		boolean databaseExists=DatabaseHelper.getDatabaseHelper().databaseExists(this.databaseName);
		if(!databaseExists)
		{
			Utils.printMissingDatabaseError(this.databaseName);
	
		}
		return databaseExists;
		
		
	}

}
