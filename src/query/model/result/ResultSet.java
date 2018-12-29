package query.model.result;

/* created by prashanth on 29/12/2018 */

import java.util.ArrayList;

public class ResultSet extends Result{
	private ArrayList<String> columns;
	private ArrayList<Record> records;
	
	public ResultSet(int rowAffected)
	{
		super(rowAffected);
		this.records=new ArrayList<>();
	}
	public static ResultSet CreateResultSet()
	{
		return new ResultSet(0);	}
	
	public void setColumns(ArrayList<String> columns)
	
	{
		this.columns=columns;
	}
	
	public void addRecord(Record record)
	{
		if(record==null)
		{
			return;
		}
		if(this.records==null)
		{
			this.records=new ArrayList<>();
			
		}
		this.records.add(record);
		this.rowsAffected++;
	}

}
