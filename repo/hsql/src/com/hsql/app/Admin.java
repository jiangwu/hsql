package com.hsql.app;

import java.io.IOException;

import com.hsql.core.AdminUtil;

public class Admin {

	public static void main(String[] args){
		if(args.length<2 ){
			System.out.println("Usage:\n create|delete tabelName [col1 col2 col3 ...]");
			return;
		}
		String tableName=args[1];
		AdminUtil admin=new AdminUtil();
		
		if(args[0].equals("create")){

			if(args.length<3){
				System.out.println("to create table, provide column list to be indexed");
				return;
			}
			String[] col=new String [args.length-2];
			for(int i=2;i<args.length;i++){
				col[i-2]=args[i];
			}

			try {
				admin.createTable(tableName, col);
			} catch (IOException e) {
				System.out.println("create table failed.");
				e.printStackTrace();
			}
			
		}else{
			try {
				admin.deleteTable(tableName);
			} catch (IOException e) {
				System.out.println("delete table failed");
				e.printStackTrace();
			}
		}
		
	}

}
