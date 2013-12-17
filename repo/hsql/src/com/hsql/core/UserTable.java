package com.hsql.core;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public interface UserTable {

	void open() throws Exception;

	void close();

	void delete(String pk) throws IOException, Exception;

	void insert(String key, Map<String, String> allCols) throws Exception;
	
	Iterable<UserRow> select(String condition) throws Exception;
	

}
