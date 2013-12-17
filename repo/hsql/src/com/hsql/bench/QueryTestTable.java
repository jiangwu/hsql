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
	public static long run() throws Exception {
		String tableName = TestSetting.tableName;
		UserTable table = UserTableFactory.getTable(tableName);


		int colSize=TestSetting.colSize;
		table.open();
		Random random = new Random();
		long totalTime=0;

		for (int j = 0; j < 100; j++) {
			long t1 = System.nanoTime();

			StringBuffer sb=new StringBuffer();
			for (String col : TestSetting.indexCol) {
				sb.append(col);
				sb.append("=");
				sb.append(String.format("%02d", random.nextInt(colSize)));
				sb.append(" and ");
			}

			Iterable<UserRow> res = table.select(sb.substring(0, sb.length()-5));
			Iterator<UserRow> it = res.iterator();
			if (it.hasNext()) {
				UserRow row = it.next();
				System.out.println("takes " + (System.nanoTime() - t1)
						+ " ns get key " + row.getKey());
			}else{
				System.out.println("Didn't find");
			}
			totalTime=totalTime+(System.nanoTime() - t1);
		}

		System.out.println("Average "+(totalTime/100)+ " ns for 1 row");
		table.close();
		
		return totalTime/100;

	}

}
