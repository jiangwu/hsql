package com.hsql.app;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.Map.Entry;

import com.hsql.core.IndexAdmin;
import com.hsql.core.IndexAdminImpl;
import com.hsql.core.UserTable;
import com.hsql.core.UserTableFactory;

public class Admin {

	private static void printHelp() {
		System.out.println(" help");
		System.out.println(" build tabelName col1 col2 col3 ...");
		System.out.println(" delete tabelName");
		System.out.println(" list");
		System.out.println(" quit");

		return;
	}

	public static void main(String[] args) throws Exception {
		run();
	}

	public static void run() throws Exception {
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		IndexAdmin admin = new IndexAdminImpl();
		admin.open();
		
		while (true) {
			System.out.print("IndexAdmin>");
			String input = br.readLine();
			String[] args = input.split(" ");


			if(args[0].equals("help")){
				printHelp();
			}else if(args[0].equals("quit")){
				br.close();
				admin.close();
				break;
			}else if(args[0].equals("list")){
				Map<String, String> indexes=admin.getIndexedTables();
				System.out.println("Table Name\t\tIndexes");
				for(Entry<String,String> e: indexes.entrySet()){
					System.out.println(e.getKey()+"\t\t"+e.getValue());
				}
			}else if (args[0].equals("build")) {
				String tableName = args[1];
				String[] col = new String[args.length - 2];
				for (int i = 2; i < args.length; i++) {
					col[i - 2] = args[i];
				}

				try {
					admin.buildIndex(tableName, col);
					System.out.println("Table " + tableName + "indexes are created.");
				} catch (Exception e) {
					System.out.println("create table index failed.");
					e.printStackTrace();
				}

			} else if (args[0].equals("delete")) {
				String tableName = args[1];
				try {
					admin.deleteIndex(tableName);
					System.out.println("Table " + tableName + "indexes are deleted.");
				} catch (IOException e) {
					System.out.println("delete table failed");
					e.printStackTrace();
				}
			}else if (args[0].equals("help")) {
				printHelp();
			}else if (args[0].equals("quit")) {
				break;
			} else {
				System.out.println("Invalid command");
			}
		}
	}

}
