package com.hsql.app;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.hsql.core.Admin;
import com.hsql.core.UserTable;

public class FillTestTable {

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		Admin admin=new Admin();
		String[] indexCol=new String[]{"c1", "c2","c3","c4","c5"}; 
		admin.deleteTable("test");
		admin.createTable("test", indexCol);
		
		UserTable table=new UserTable();
		table.open("test");
		Map<String, String> iColVal=new HashMap<String, String>();
		int [] i=new int[5];
		int key=0;
		for( i[0]=0;i[0]<16;i[0]++){
			iColVal.put(indexCol[0], String.format("%02d", i[0]));
			for( i[1]=0;i[1]<16;i[1]++){
				iColVal.put(indexCol[1], String.format("%02d", i[1]));
				for( i[2]=0;i[2]<16;i[2]++){
					iColVal.put(indexCol[2], String.format("%02d", i[2]));
					for( i[3]=0;i[3]<16;i[3]++){
						iColVal.put(indexCol[3], String.format("%02d", i[3]));
						for( i[4]=0;i[4]<16;i[4]++){
							iColVal.put(indexCol[4], String.format("%02d", i[4]));

								table.insert(key+"", iColVal);
								key++;

						}
					}
				}
				
			}
			
		}
		table.close();
		
		
		

	}

}
