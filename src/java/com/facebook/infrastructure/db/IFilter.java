package com.facebook.infrastructure.db;

import java.io.DataInputStream;
import java.io.IOException;

import com.facebook.infrastructure.io.DataInputBuffer;
import com.facebook.infrastructure.io.SSTable;

public interface IFilter
{
	public boolean isDone();
	public ColumnFamily filter(String cfName, ColumnFamily cf);
    public IColumn filter(IColumn column, DataInputStream dis) throws IOException;
    public DataInputBuffer next(String key, String cf, SSTable ssTable) throws IOException;
}
