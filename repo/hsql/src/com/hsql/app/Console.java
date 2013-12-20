package com.hsql.app;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.hsql.core.AdminUtil;
import com.hsql.core.UserRow;
import com.hsql.core.UserTable;
import com.hsql.core.UserTableFactory;
import com.hsql.core.Utils;

public class Console {


	public static void main(String[] args) {
		try{
			BufferedReader br = 
	                      new BufferedReader(new InputStreamReader(System.in));
	 
			String input;


			while(true){
				System.out.print(">");
				input=br.readLine();
				if(input.equals("quit"))
					break;
				String[] token=input.trim().split(" ");
				if(token[0].equals("help")){
					System.out.println(" show index tableName");
					System.out.println(" put tableName primaryKey index1=value1 ...");
					System.out.println(" get tableName index1=value1 ...");
					System.out.println(" delete tableName primaryKey");
					System.out.println(" quit");
				}else if(token[0].equals("delete")){
					String tableName=token[1];
					String pk=token[2];
					UserTable userTable = UserTableFactory.getTable(tableName);;
					try {
						userTable.open();
					} catch (Exception e) {
						System.out.println("cannot open table");
						continue;
					}
					try {
						userTable.deleteRow(pk);
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					userTable.close();
					
				}else if(token[0].equals("get")){
					String tableName=token[1];
					 UserTable userTable = UserTableFactory.getTable(tableName);;
					try {
						userTable.open();
					} catch (Exception e1) {
						System.out.println("cannot open table");
						continue;
					}
					
					String[] ss = input.split(tableName);
					
					try {

						Iterable<UserRow> res = userTable.select(ss[1]);

						for(UserRow row:res){
							
							Utils.printRow(row);
						}
//						Iterator<UserRow> it = res.iterator();
//						while(it.hasNext()){
//							Utils.printRow(it.next());
//									
//						}
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					userTable.close();
				}else if(token[0].equals("put")){
					String tableName=token[1];
					String key=token[2];
					 UserTable userTable = UserTableFactory.getTable(tableName);;
					try {
						userTable.open();
					} catch (Exception e1) {
						System.out.println("cannot open table");
						continue;
					}
					StringBuffer sb=new StringBuffer();

					for(int i=3;i<token.length;i++){
						String[]kv=token[i].split("=");

						sb.append(kv[0].trim());
						sb.append("=");
						sb.append(kv[1].trim());
						sb.append(" ");
					}
					try {
						userTable.insert(key, sb.toString());
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					userTable.close();
				}else if(token[0].equals("show")){
					if(token[1].equals("index")){
						String tableName=token[2];
						AdminUtil admin=new AdminUtil();
						String [] cols=admin.getIndexCols(tableName);
						for(String col:cols){
							System.out.print(col+" ");
						}
						System.out.println();
						
					}else{
						
					}
					
				}else{
					System.out.println("Invalid command");
				}
				
			}
	 
			
	 
		}catch(IOException io){
			io.printStackTrace();
		}

	}

}
