package com.hsql.app;

import java.util.HashMap;
import java.util.Map;

import com.hsql.core.UserRow;
import com.hsql.core.UserTable;
import com.hsql.core.UserTableFactory;
import com.hsql.core.Utils;

public class QueryTestTable {

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		UserTable table=UserTableFactory.getTable("test");;
			
		table.open();
		
		long t1=System.currentTimeMillis();
		Map<String, String> indexes=new HashMap<String, String>();
		

		indexes.put("c2","01");
		indexes.put("c3","02");
		indexes.put("c4","03");
		indexes.put("c5","04");
		
		Iterable<UserRow> res = table.select(indexes);
		
		int count=0;
		for(UserRow row:res){
			Utils.printRow(row);
			count ++;
		}
		
		System.out.println(System.currentTimeMillis()-t1+ "ms, "+ count+ " rows");
		
		Iterable<UserRow> rows=table.select(indexes);
		
		System.out.println("use iterator...");
		t1=System.currentTimeMillis();
		count =0;
		for(UserRow row:rows){
			Utils.printRow(row);
			count++;
		}
		System.out.println(System.currentTimeMillis()-t1+ "ms, "+ count+ " rows");
		table.close();
		

	}

}
