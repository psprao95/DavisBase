package query.ddl;

import query.model.result.Result;
import query.base.IQuery;
import query.model.parser.Condition;
import query.dml.*;
import common.Utils;
import common.DatabaseHelper;

import java.util.ArrayList;
import java.io.File;

import common.DatabaseConstants;

public class DropTableQuery implements IQuery {
	public String databaseName;
	public String tableName;
	
	public DropTableQuery(String databaseName,String tableName)
	{
		this.databaseName=databaseName;
		this.tableName=tableName;
	}
	
	@Override
	public Result ExecuteQuery()
	{
		ArrayList<Condition> conditionList = new ArrayList<>();
		conditionList.add(Condition.CreateCondition(String.format("database name=%s", this.databaseName)));
		conditionList.add(Condition.CreateCondition(String.format("database name=%s", this.tableName)));
		
		IQuery deleteQuery = new DeleteQuery(DatabaseConstants.DEFAULT_CATALOG_DATABASENAME,DatabaseConstants.SYSTEM_TABLES_TABLENAME,conditionList,true);
		deleteQuery.ExecuteQuery();
		
		deleteQuery = new DeleteQuery(DatabaseConstants.DEFAULT_CATALOG_DATABASENAME,DatabaseConstants.SYSTEM_COLUMNS_TABLENAME,conditionList,true);
		deleteQuery.ExecuteQuery();
		
		File table = new File(String.format("%s/%s/%s%s", DatabaseConstants.DEFAULT_DATA_DIRNAME,this.databaseName,this.tableName,DatabaseConstants.DEFAULT_FILE_EXTENSION));
		if(!Utils.RecursivelyDelete(table))
		{
			Utils.printMessage(String.format("Error(200): Unable to delete %s.%s", this.databaseName,this.tableName));
			return null;
		}
		return new Result(1);
	}
	
	@Override
	public boolean ValidateQuery()
	{
		if(!DatabaseHelper.getDatabaseHelper().tableExists(this.tableName,this.tableName))
		{
			Utils.printMissingTableError(this.databaseName,this.tableName);
			return false;
		}
		return true;
	}
}
