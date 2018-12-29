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
	}

}
