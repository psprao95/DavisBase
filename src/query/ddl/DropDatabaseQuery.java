package query.ddl;

/**
 * Created by prashanth on 29/12/2018
 */
import query.QueryHandler;
import query.base.IQuery;
import query.model.result.Result;
import common.Utils;
import query.model.parser.Condition;
import query.dml.DeleteQuery;
import common.DatabaseConstants;
import common.DatabaseHelper;

import java.io.File;
import java.util.*;

public class DropDatabaseQuery implements IQuery{
	public String databaseName;
	
	public DropDatabaseQuery(String databaseName)
	{
		this.databaseName=databaseName;
	}
	
	@Override
	public Result ExecuteQuery()
	{
		File database = new File(Utils.getDatabasePath(databaseName));
		Condition condition = Condition.CreateCondition(String.format("database_name = '%s'",this.databaseName));
		ArrayList<Condition> conditions = new ArrayList<>();
		conditions.add(condition);
		
		IQuery deleteEntryQuery=new DeleteQuery(DatabaseConstants.DEFAULT_CATALOG_DATABASENAME,DatabaseConstants.SYSTEM_TABLES_TABLENAME,conditions,true);
		deleteEntryQuery.ExecuteQuery();
		
		deleteEntryQuery=new DeleteQuery(DatabaseConstants.DEFAULT_CATALOG_DATABASENAME,DatabaseConstants.SYSTEM_COLUMNS_TABLENAME,conditions,true);
		deleteEntryQuery.ExecuteQuery();
		
		boolean isDeleted = Utils.RecursivelyDelete(database);
		
		if(!isDeleted)
		{
			Utils.printMessage(String.format("Error (200): Unable to deleted database %s", this.databaseName));
			return null;
		}
		
		if(QueryHandler.ActiveDatabaseName==this.databaseName)
		{
			QueryHandler.ActiveDatabaseName="";
		}
		
		Result result = new Result(1);
		return result;
	}
	
	@Override
	public boolean ValidateQuery()
	{
		boolean existsDatabase = DatabaseHelper.getDatabaseHelper().databaseExists(databaseName);
		if(!existsDatabase)
		{
			Utils.printMissingDatabaseError(this.databaseName);
			return false;
		}
		return true;
	}

}
