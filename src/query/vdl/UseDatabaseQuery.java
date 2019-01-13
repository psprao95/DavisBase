package query.vdl;
import common.DatabaseHelper;
import common.Utils;
import query.QueryHandler;
import query.base.IQuery;
import query.model.result.Result;

public class UseDatabaseQuery implements IQuery {
	public String databaseName;
	public UseDatabaseQuery(String databaseName)
	{
		this.databaseName=databaseName;
	}
	
	@Override
	public Result ExecuteQuery()
	{
		QueryHandler.ActiveDatabaseName=this.databaseName;
		Utils.printMessage("Database changed");
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
