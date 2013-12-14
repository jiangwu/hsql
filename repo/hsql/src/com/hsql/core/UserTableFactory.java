package com.hsql.core;

public class UserTableFactory {
	public static UserTable getTable(String tableName){
		
		return new UserTableImpl(tableName);
	}
	

}
