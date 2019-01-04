package query;

import query.base.IQuery;

public class QueryParser {
	public static boolean isExit=false;
	
	public static void parseCommand(String userCommand)
	{
		if(userCommand.toLowerCase().equals(QueryHandler.SHOW_TABLES_COMMAND.toLowerCase()))
		{
			IQuery query = QueryHandler.ShowTableListQueryHandler();
			QueryHandler.ExecuteQuery(query);
		}
		else if(userCommand.toLowerCase().equals(QueryHandler.SHOW_DATABASES_COMMAND.toLowerCase()))
		{
			IQuery query = QueryHandler.ShowDatabaseQueryHandler();
			QueryHandler.ExecuteQuery(query);
		}
		else if(userCommand.toLowerCase().equals(QueryHandler.HELP_COMMAND.toLowerCase()))
		{
			QueryHandler.HelpQueryHandler();
			
		}
		else if(userCommand.toLowerCase().equals(QueryHandler.VERSION_COMMAND.toLowerCase()))
		{
			 QueryHandler.ShowVersionQueryHandler();
			
		}
		else if(userCommand.toLowerCase().equals(QueryHandler.EXIT_COMMAND.toLowerCase()))
		{
			System.out.println("Exiting...");
			isExit=true;
		}
		else if(userCommand.toLowerCase().equals(QueryHandler.USE_DATABASE_COMMAND.toLowerCase()))
		{
			if(!PartsEqual(userCommand,QueryHandler.USE_DATABASE_COMMAND))
			{
				QueryHandler.UnrecognisedCommand(userCommand, QueryHandler.USE_HELP_MESSAGE);
				return;
			}
			 String databaseName = userCommand.substring(QueryHandler.USE_DATABASE_COMMAND.length());
			 IQuery query = QueryHandler.UseDatabaseQueryHandler(databaseName.trim());
			 QueryHandler.ExecuteQuery(query);
			
		}
		
		else if(userCommand.toLowerCase().equals(QueryHandler.DESC_TABLE_COMMAND.toLowerCase()))
		{
			 
			if(!PartsEqual(userCommand,QueryHandler.DESC_TABLE_COMMAND))
			{
				QueryHandler.UnrecognisedCommand(userCommand, QueryHandler.USE_HELP_MESSAGE);
				return;
			}
			 String tableName = userCommand.substring(QueryHandler.DESC_TABLE_COMMAND.length());
			 IQuery query = QueryHandler.UseDatabaseQueryHandler(tableName.trim());
			 QueryHandler.ExecuteQuery(query);
		}
		
		else if(userCommand.toLowerCase().startsWith(QueryHandler.DROP_TABLE_COMMAND.toLowerCase()))
		{
            if(!PartsEqual(userCommand, QueryHandler.DROP_TABLE_COMMAND))
            {
                QueryHandler.UnrecognisedCommand(userCommand, QueryHandler.USE_HELP_MESSAGE);
                return;
            }
		 String tableName = userCommand.substring(QueryHandler.DROP_TABLE_COMMAND.length());
		 IQuery query = QueryHandler.UseDatabaseQueryHandler(tableName.trim());
		 QueryHandler.ExecuteQuery(query);
		}
		
		else if(userCommand.toLowerCase().startsWith(QueryHandler.DROP_DATABASE_COMMAND.toLowerCase()))
		{
            if(!PartsEqual(userCommand, QueryHandler.DROP_DATABASE_COMMAND))
            {
                QueryHandler.UnrecognisedCommand(userCommand, QueryHandler.USE_HELP_MESSAGE);
                return;
            }
		 String databaseName = userCommand.substring(QueryHandler.DROP_DATABASE_COMMAND.length());
		 IQuery query = QueryHandler.UseDatabaseQueryHandler(databaseName.trim());
		 QueryHandler.ExecuteQuery(query);
		}
		
		else if(userCommand.toLowerCase().startsWith(QueryHandler.SELECT_COMMAND.toLowerCase()))
		{
            if(!PartsEqual(userCommand, QueryHandler.SELECT_COMMAND))
            {
                QueryHandler.UnrecognisedCommand(userCommand, QueryHandler.USE_HELP_MESSAGE);
                return;
            }
            
            int index=userCommand.toLowerCase().indexOf("from");
            if(index==-1)
            {
            	QueryHandler.UnrecognisedCommand(userCommand, "Expected FROM keyword");
            	return;
            }
            
            String attributeList = userCommand.substring(QueryHandler.SELECT_COMMAND.length(), index).trim();
            String  restUserQuery=userCommand.substring(index+"from".length());
            
            index=restUserQuery.toLowerCase().indexOf("where");
            if(index==-1)
            {
            	String tableName=restUserQuery.trim();
            	IQuery query = QueryHandler.SelectQueryHandler(attributeList.split(","), tableName, "");
            	QueryHandler.ExecuteQuery(query);
            	return;
            }
            
            String tableName=restUserQuery.substring(0,index);
            String conditions = restUserQuery.substring(index+"where".length());
            IQuery query = QueryHandler.SelectQueryHandler(attributeList.split(","), tableName, conditions);
            QueryHandler.ExecuteQuery(query);
		}
		
		
	}
	
	private static boolean PartsEqual(String userCommand, String expectedCommand)
	{
		String[] userParts=userCommand.toLowerCase().split(" ");
		String[] actualParts = expectedCommand.toLowerCase().split(" ");
		for(int i=0;i<userParts.length;i++)
		{
			if(!userParts[i].equals(actualParts[i]))
			{
				return false;
			}
		}
		return true;
	}

}
