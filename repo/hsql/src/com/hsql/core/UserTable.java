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


	void insert(String key, String colValues) throws Exception;
	Iterable<UserRow> select(String condition) throws Exception;




//	void insertIndex(String key, Map<String, String> indexCol) throws Exception;

	void reBuildIndex() throws Exception;
	

}
