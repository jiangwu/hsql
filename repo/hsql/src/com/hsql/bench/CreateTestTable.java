package com.hsql.bench;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import com.hsql.core.IndexAdminImpl;
import com.hsql.core.UserTable;
import com.hsql.core.UserTableFactory;

public class CreateTestTable {

	/**
	 * @param args
	 * @throws Exception
	 */
	public static void create() throws Exception {
		String tableName = TestSetting.tableName;
		IndexAdminImpl admin = new IndexAdminImpl();

		HBaseAdmin hbaseAdmin = new HBaseAdmin(new Configuration());
		if (hbaseAdmin.isTableAvailable(tableName)) {

			admin.deleteIndex(tableName);
		}
		hbaseAdmin.close();

		String[] indexCol = TestSetting.indexCol;

		admin.buildIndex(tableName, indexCol);

		UserTable table = UserTableFactory.getTable(tableName);
		table.open();

		int[] i = new int[5];
		int key = 0;
		int colSize=TestSetting.colSize;
		
		for (i[0] = 0; i[0] < colSize; i[0]++) {

			for (i[1] = 0; i[1] < colSize; i[1]++) {

				for (i[2] = 0; i[2] < colSize; i[2]++) {

					for (i[3] = 0; i[3] < colSize; i[3]++) {

						for (i[4] = 0; i[4] < colSize; i[4]++) {
							StringBuffer sb=new StringBuffer();
							for(int j=0;j<5;j++){
								
								sb.append(indexCol[j]);
								sb.append("=");
								sb.append(String.format("%02d", i[j]));
								sb.append(" ");
							}

							table.insert(key + "", sb.toString());
							key++;

						}
					}
				}

			}

		}
		table.close();
	}

}
