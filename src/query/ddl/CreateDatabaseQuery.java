package query.ddl;

/* 
 * Created by prashanth on 29/12/2018
 */
import query.base.IQuery;
import query.model.result.Result;
import common.DatabaseHelper;
import common.Utils;

import java.io.File;

public class CreateDatabaseQuery implements IQuery{
	public String databaseName;
	
	public CreateDatabaseQuery(String databaseName)
	{
		this.databaseName=databaseName;
	}
	
	@Override 
	public Result  ExecuteQuery()
	{
		File database = new File(Utils.getDatabasePath(databaseName));
		boolean isCreated = database.mkdir();
		
		if(!isCreated)
		{
			System.out.println(String.format("Error(200): Unable to create database '%s'",this.databaseName));
			return null;
		}
		Result result = new Result(1);
		return result;

	}
	
	@Override 
	public boolean ValidateQuery()
	{
		boolean databaseExists = DatabaseHelper.getDatabaseHelper().databaseExists(databaseName);
		if(databaseExists)
		{
			System.out.println(String.format("Error(104D): Database '%s' already exists",this.databaseName));
			return false;
		}
		return true;
	}
	
}
