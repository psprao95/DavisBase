package io.model;

import java.util.List;
import java.util.ArrayList;

import datatypes.*;
public class DataRecord {
	private List<Object> columnValueList;
	private short size;
	private int rowId;
	private int pageLocated;
	private short offset;
	
	public DataRecord()
	{
		size=0;
		columnValueList=new ArrayList<>();
		pageLocated=-1;
		offset=-1;
	}
	
	public List<Object> getColumnValueList()
	{
		return columnValueList;
	}
	
	public short getSize()
	{
		return size;
	}
	
	public void setSize(short size)
	{
		this.size=size;
		
	}
	
	
	public short getHeaderSize()
	{
		return (short)(Short.BYTES+Integer.BYTES);
	}

}
