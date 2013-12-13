package com.hsql.core;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
				if(token[0].equals("list")){
					System.out.println("t1");
					System.out.println("t1");
				}else if(token[0].equals("get")){
					String tableName=token[1];
					 UserTable userTable = new UserTable();
					userTable.open(tableName);
					Map<String, String> cols=new HashMap<String, String>();
					for(int i=2;i<token.length;i++){
						String[]kv=token[i].split("=");
						cols.put(kv[0].trim(), kv[1].trim());
					}
					try {
						List<UserRow> res = userTable.query(cols);
						for(UserRow row:res){
							Utils.printRow(row);
									
						}
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					userTable.close();
				}else if(token[0].equals("put")){
					String tableName=token[1];
					String key=token[2];
					 UserTable userTable = new UserTable();
					userTable.open(tableName);
					Map<String, String> cols=new HashMap<String, String>();
					for(int i=3;i<token.length;i++){
						String[]kv=token[i].split("=");
						cols.put(kv[0].trim(), kv[1].trim());
					}
					try {
						userTable.insert(key, cols);
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					userTable.close();
				}
				
			}
	 
			
	 
		}catch(IOException io){
			io.printStackTrace();
		}

	}

}