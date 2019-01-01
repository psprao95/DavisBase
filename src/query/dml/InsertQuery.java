package query.dml;

import java.util.ArrayList;
import query.base.IQuery;
import query.model.parser.Literal;

public class InsertQuery implements IQuery{
	public String tableName;
	public ArrayList<String> columns;
	public ArrayList<Literal> values;
	public String databaseName;
	
	public InsertQuery(String databaseName, String tableName,ArrayList<String > columns, ArrayList<Literal> values)
	{
		this.tableName=tableName;
		this.values=values;
		this.columns=columns;
		this.databaseName=databaseName;
	}
	

}
