package com.hsql.app;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.hsql.core.UserRow;
import com.hsql.core.UserTable;
import com.hsql.core.Utils;

public class QueryTestTable {

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		UserTable table=new UserTable();
			
		table.open("test");
		
		long t1=System.currentTimeMillis();
		Map<String, String> indexes=new HashMap<String, String>();
		

		indexes.put("c2","01");
		indexes.put("c3","02");
		indexes.put("c4","03");
//		indexes.put("c5","04");
		
		List<UserRow> res = table.query(indexes);
		
		for(UserRow row:res){
			Utils.printRow(row);
		}
		
		System.out.println(System.currentTimeMillis()-t1+ "ms, "+ res.size()+ " rows");
		
		table.close();
		

	}

}
