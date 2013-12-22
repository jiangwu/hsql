package com.hsql.app;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import com.hsql.core.UserRow;
import com.hsql.core.UserTable;
import com.hsql.core.UserTableFactory;
import com.hsql.core.Utils;

public class Console {
	static Map<String, UserTable> tables = new HashMap<String, UserTable>();

	private static UserTable getTable(String tableName) throws Exception {
		if (!tables.containsKey(tableName)) {
			UserTable t = UserTableFactory.getTable(tableName);
			;
			t.open();
			tables.put(tableName, t);
		}
		return tables.get(tableName);
	}

	public static void main(String[] args) throws Exception {

		try {
			BufferedReader br = new BufferedReader(new InputStreamReader(
					System.in));

			String input;

			while (true) {
				System.out.print(">");
				input = br.readLine();
				if (input.equals("quit")) {
					for (UserTable t : tables.values()) {
						t.close();
					}
					break;
				}
				String[] token = input.trim().split(" ");
				if (token[0].equals("help")) {
					System.out
							.println(" put tableName primaryKey index1=value1 ...");
					System.out.println(" get tableName index1=value1 ...");
					System.out.println(" delete tableName primaryKey");
					System.out.println(" quit");
				} else if (token[0].equals("delete")) {
					String tableName = token[1];

					String pk = token[2];

					getTable(tableName).deleteRow(pk);

				} else if (token[0].equals("get")) {
					String tableName = token[1];

					String[] ss = input.split(tableName);

					try {

						Iterable<UserRow> res = getTable(tableName).select(
								ss[1]);

						for (UserRow row : res) {

							Utils.printRow(row);
						}

					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}

				} else if (token[0].equals("put")) {
					String tableName = token[1];
					String key = token[2];

					StringBuffer sb = new StringBuffer();

					for (int i = 3; i < token.length; i++) {
						String[] kv = token[i].split("=");

						sb.append(kv[0].trim());
						sb.append("=");
						sb.append(kv[1].trim());
						sb.append(" ");
					}

					getTable(tableName).insert(key, sb.toString());

				} else {
					System.out.println("Invalid command");
				}

			}

		} catch (IOException io) {
			io.printStackTrace();
		}

	}

}
