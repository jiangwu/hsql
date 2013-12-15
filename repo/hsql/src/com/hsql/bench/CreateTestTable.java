package com.hsql.bench;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import com.hsql.core.AdminUtil;
import com.hsql.core.UserTable;
import com.hsql.core.UserTableFactory;

public class CreateTestTable {

	/**
	 * @param args
	 * @throws Exception
	 */
	public static void run() throws Exception {
		String tableName = TestSetting.tableName;
		AdminUtil admin = new AdminUtil();

		HBaseAdmin hbaseAdmin = new HBaseAdmin(new Configuration());
		if (hbaseAdmin.isTableAvailable(tableName)) {

			admin.deleteTable(tableName);
		}
		hbaseAdmin.close();

		String[] indexCol = TestSetting.indexCol;

		admin.createTable(tableName, indexCol);

		UserTable table = UserTableFactory.getTable(tableName);
		table.open();
		Map<String, String> iColVal = new HashMap<String, String>();
		int[] i = new int[5];
		int key = 0;
		int colSize=TestSetting.colSize;
		for (i[0] = 0; i[0] < colSize; i[0]++) {
			iColVal.put(indexCol[0], String.format("%02d", i[0]));
			for (i[1] = 0; i[1] < colSize; i[1]++) {
				iColVal.put(indexCol[1], String.format("%02d", i[1]));
				for (i[2] = 0; i[2] < colSize; i[2]++) {
					iColVal.put(indexCol[2], String.format("%02d", i[2]));
					for (i[3] = 0; i[3] < colSize; i[3]++) {
						iColVal.put(indexCol[3], String.format("%02d", i[3]));
						for (i[4] = 0; i[4] < colSize; i[4]++) {
							iColVal.put(indexCol[4],
									String.format("%02d", i[4]));

							table.insert(key + "", iColVal);
							key++;

						}
					}
				}

			}

		}
		table.close();
	}

}
