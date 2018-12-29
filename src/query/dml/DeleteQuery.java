package query.dml;

import query.base.*;
import query.model.parser.*;
import java.util.*;

public class DeleteQuery implements IQuery {
	
	public String databaseName;
	public String tableName;
	public ArrayList<Condition> conditions;
	public boolean isInternal=false;
	
	public DeleteQuery(String databaseName,String tableName, ArrayList<Condition> conditions)
	{
		this.databaseName=databaseName;
		this.tableName=tableName;
		this.conditions=conditions;
	}
	public DeleteQuery(String databaseName,String tableName, ArrayList<Condition> conditions, boolean isInternal)
	{
		this.databaseName=databaseName;
		this.tableName=tableName;
		this.conditions=conditions;
		this.isInternal=isInternal;
	}

}
