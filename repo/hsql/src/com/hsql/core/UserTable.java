package com.hsql.core;

import java.io.IOException;
import java.util.Map;

public interface UserTable {

	void open() throws Exception;

	void close();

	void delete(String pk) throws IOException;

	void insert(String key, Map<String, String> allCols) throws Exception;

	Iterable<UserRow> select(Map<String, String> indexes) throws Exception;

}
