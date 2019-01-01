package query;

import common.DatabaseConstants;
import query.base.IQuery;
import query.ddl.*;
import query.vdl.SelectQuery;
import query.model.parser.Condition;
import query.model.result.Result;

import java.util.ArrayList;
public class QueryHandler {
	static final String SELECT_COMMAND= "SELECT";
	static final String DROP_TABLE_COMMAND= "DROP TABLE";
	 static final String DROP_DATABASE_COMMAND= "DROP DATABASE";
	 static final String HELP_COMMAND= "HELP";
	 static final String VERSION_COMMAND= "VERSION";
	 static final String EXIT_COMMAND= "EXIT";
	 static final String SHOW_TABLES_COMMAND= "SHOW TABLES";
	 static final String SHOW_DATABASES_COMMAND= "SHOW DATABASES";
	 static final String INSERT_COMMAND= "INSERT INTO";
	 static final String DELETE_COMMAND= "DELETE FROM";
	 static final String UPDATE_COMMAND= "UPDATE";
	 static final String CREATE_TABLE_COMMAND= "CREATE TABLE";
	 static final String CREATE_DATABASE_COMMAND= "CREATE DATABASE";
	 static final String USE_DATABASE_COMMAND= "USE";
	static final String DESC_TABLE_COMMAND= "DESC";
	private static final String NO_DATABASES_SELECTED_MESSAGE= "No databases selected";
	public static final String USE_HELP_MESSAGE="\n Type help; to display supported commands.";
	
	public static String ActiveDatabaseName="";
	
	public static String getVersion()
	{
		return DatabaseConstants.VERSION;
	}
	
	private static String getCopyright()
	{
		return DatabaseConstants.COPYRIGHT;
	}
	
	public static String line(String s,int num)
	{
		String a="";
		for(int i=0;i<num;i++)
		{
			a+=s;
		}
		return a;
	}

	static IQuery ShowTableListQueryHandler()
	{
		if(QueryHandler.ActiveDatabaseName.equals(""))
		{
			System.out.println(QueryHandler.NO_DATABASES_SELECTED_MESSAGE);
			return null;
		}
		return new ShowTableQuery(QueryHandler.ActiveDatabaseName);
	}
	
	static IQuery DropTableQueryHandler(String tableName)
	{
		if(QueryHandler.ActiveDatabaseName.equals(""))
		{
			System.out.println(QueryHandler.NO_DATABASES_SELECTED_MESSAGE);
			return null;
		}
		return new DropTableQuery(QueryHandler.ActiveDatabaseName,tableName);
	}
	
	
	
	public static void UnrecognisedCommand(String userCommand, String message)
	{
		System.out.println("Erro(100): Unrecognised Command "+ userCommand);
		System.out.println("Message: "+message);
		
	}
	
	static IQuery SelectQueryHandler(String[] attributes, String tableName, String conditionString)
	{
		if(QueryHandler.ActiveDatabaseName=="")
		{
			System.out.println(QueryHandler.NO_DATABASES_SELECTED_MESSAGE);
			return null;
		}
		
		boolean isSelectAll=false;
		SelectQuery query;
		ArrayList<String> columns = new ArrayList<>();
		for(String attribute:attributes)
		{
			columns.add(attribute.trim());
		}
		
		if(columns.size()==1 &&columns.get(0).equals("*"))
		{
			isSelectAll=true;
			columns=null;
		}
		if(conditionString.equals(""))
		{
			query = new SelectQuery(QueryHandler.ActiveDatabaseName,tableName,columns,null,isSelectAll);
			return query;
		}
		
		Condition condition=Condition.CreateCondition(conditionString);
		if(condition==null)
		{
			return null;
		}
		
		ArrayList<Condition> conditionList = new ArrayList<>();
		conditionList.add(condition);
		query=new SelectQuery(QueryHandler.ActiveDatabaseName,tableName,columns,conditionList,isSelectAll);
		return query;
	}
	
	public static void ShowVersionQueryHandler()
	{
		System.out.println("DavisBaseLite Version "+getVersion());
		System.out.println(getCopyright());
	}
	
	static void HelpQueryHandler()
	{
		System.out.println(line("*",80));
		System.out.println("Supported Commands");
		System.out.println("All commands shown below are case insensitive");
		System.out.println();
		System.out.println("\tUSE DATABASE database_name;                       Changes current database"); 
		System.out.println("\tCREATE DATABASE database_name;                    creates an empty database.");
		System.out.println("\tSHOW DATABASES;                                   SHOWS ALL DATABASES.");
		System.out.println("\tDROP DATABASE database_name;                      Removes a database");
		System.out.println("\tSHOW TABLES;                      			    Displays all tab;es in the database");
		System.out.println("\tDESC tablename;                     			    Displays table schemaRemoves a database");
		System.out.println("\tCREATE TABLE table_name (;                        Creates a table in the current database");
		System.out.println("\t\t <column name> <datatype> [primary key | not null]");
		System.out.println("\t\t...)");
		System.out.println("DROP TABEL table_name							    Drops a table from the current database;");
		System.out.println("\tSELECT <columnlist> FROM table_name               Display records whose row id is <id>");
		System.out.println("\t\t[WHERE row id=<value>];");
		System.out.println("\tINSERT INTO table_name							Insets a record into the table. ");
		System.out.println("\t\t[<column1>,<column2>..] VALUES (<value1>,<value2>...);");
		System.out.println("DELETE FROM TABLE [WHERE condition];				Deletes records from the table from");
		System.out.println("UPDATE TABLE SET <conditions>						Updates records from the table");	
		System.out.println("\t\t[WHERE CONDITION]");
		System.out.println("\tHELP; 											Displays help information");
		System.out.println("EXIT;												Exits the program");
		System.out.println();
		System.out.println();
		System.out.println(line("*",80));
	}
	
	static IQuery InsertQueryHandler(String tableName,String columnsList,String valuesList)
	{
		if(QueryHandler.ActiveDatabaseName.equals(""))
		{
			System.out.println(QueryHandler.NO_DATABASES_SELECTED_MESSAGE);
			return null;
		}
		IQuery query=null;
		ArrayList<String> columns=null;
		ArrayList<Literal> values = new ArrayList<>();
	}
	public static void ExecuteQuery(IQuery query)
	
	{
		if(query!=null && query.ValidateQuery())
		{
			Result result = query.ExecuteQuery();
			if(result!=null)
			{
				//result.Display();
			}
		}
		
	}
	}