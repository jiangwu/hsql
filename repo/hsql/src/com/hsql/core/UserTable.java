package com.hsql.core;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.ResultScanner;

public interface UserTable {

	void open() throws Exception;

	void close();

	void deleteRow(String pk) throws IOException, Exception;

	/**
	 * 
	 * @param key
	 * 				is the primary key of the inserted row
	 * @param colValues
	 * 				is a string such as "col1=value1 and col2=value2"
	 * @throws Exception
	 */
	void insert(String key, String colValues) throws Exception;

	/**
	 * 
	 * @param condition
	 *            is a string such as "col1=value1 and col2=value2 or col3=value3"
	 *            and col4=value4
	 * @return
	 * @throws Exception
	 */
	Iterable<UserRow> select(String condition) throws Exception;

}
