package query.vdl;

import java.util.ArrayList;

import query.base.IQuery;
import query.model.result.*;
import query.model.parser.*;

public class SelectQuery implements IQuery {
	String databaseName;
	String tableName;
	public ArrayList<String> columns;
	private boolean isSelectAll;
	private ArrayList<Condition> conditions = new ArrayList<>();
	
	public SelectQuery(String databaseName, String tableName,ArrayList<String> columns, ArrayList<Condition> conditions, boolean isSelectAll)
	{
		this.databaseName=databaseName;
		this.tableName=tableName;
		this.columns=columns;
		this.conditions=conditions;
		this.isSelectAll=isSelectAll;
	}
	
	
	@Override
	public Result ExecuteQuery()
	{
		
	}
	
	@Override 
	public boolean ValidateQuery()
	{
		
	}

}
