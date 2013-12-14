package com.hsql.bench;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;

import com.hsql.core.UserRow;
import com.hsql.core.UserTable;
import com.hsql.core.UserTableFactory;

public class QueryTestTable {

	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		String tableName = TestSetting.tableName;
		UserTable table = UserTableFactory.getTable(tableName);


		table.open();
		Random random = new Random();
		long totalTime=0;

		for (int j = 0; j < 100; j++) {
			long t1 = System.currentTimeMillis();
			Map<String, String> indexes = new HashMap<String, String>();

			for (String col : TestSetting.indexCol) {
				indexes.put(col, String.format("%02d", random.nextInt(16)));
			}

			Iterable<UserRow> res = table.select(indexes);
			Iterator<UserRow> it = res.iterator();
			if (it.hasNext()) {
				UserRow row = it.next();
				System.out.println("takes " + (System.currentTimeMillis() - t1)
						+ " ms get key " + row.getKey());
			}else{
				System.out.println("Didn't find");
			}
			totalTime=totalTime+(System.currentTimeMillis() - t1);
		}

		System.out.println("Average "+(totalTime/100)+ " for 1 row");
		table.close();

	}

}
