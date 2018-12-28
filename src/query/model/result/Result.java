package query.model.result;

public class Result {
	int rowsAffected;
	private boolean isInternal=false;
	
	public Result(int rowsAffected)
	{
		this.rowsAffected=rowsAffected;
	}

}
