package com.hsql.core;

import java.util.Map;

public class UserRow {
	
	private String key;
	private Map<String,String> indexedCols;
	private Map<String,String> unIndexedCols;
	public String getKey() {
		return key;
	}
	public void setKey(String key) {
		this.key = key;
	}
	public Map<String, String> getIndexedCols() {
		return indexedCols;
	}
	public void setIndexedCols(Map<String, String> indexedCols) {
		this.indexedCols = indexedCols;
	}
	public Map<String, String> getNonIndexedCols() {
		return unIndexedCols;
	}
	public void setNonIndexedCols(Map<String, String> nonIndexedCols) {
		this.unIndexedCols = nonIndexedCols;
	}



}
