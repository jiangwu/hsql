package com.hsql.core;

import java.util.Map;

public class Utils {

	public static void printRow(UserRow row){
		System.out.println("row key:"+row.getKey());
		Map<String, String> map=row.getIndexedCols();
		for(String k:map.keySet()){
			System.out.println(k+":"+map.get(k));
		}
		
		map=row.getNonIndexedCols();
		for(String k:map.keySet()){
			System.out.println(k+":"+map.get(k));
		}
	}

}
