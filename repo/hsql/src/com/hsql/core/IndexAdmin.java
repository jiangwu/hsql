package com.hsql.core;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public interface IndexAdmin {
	void open() throws IOException;
	void close() throws IOException;

	String[] getIndexCols(String tableName) throws IOException;

	void buildIndex(String table, String[] cols) throws Exception;

	void deleteIndex(String tableName) throws IOException;
	Map<String, String> getIndexedTables() throws IOException;

	boolean isTableIndexed(String tableName) throws IOException;

}
