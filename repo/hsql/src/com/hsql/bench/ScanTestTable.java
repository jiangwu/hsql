package com.hsql.bench;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;

public class ScanTestTable {

	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		String tableName = TestSetting.tableName;
		HTable table = new HTable(tableName);

		long totalTime=0;
		for (int j = 0; j < 100; j++) {

			Random random = new Random();
			FilterList list = new FilterList(FilterList.Operator.MUST_PASS_ALL);
			for (String col : TestSetting.indexCol) {

				SingleColumnValueFilter filter = new SingleColumnValueFilter(
						"f1".getBytes(), col.getBytes(), CompareOp.EQUAL,
						String.format("%02d", random.nextInt(16)).getBytes());
				list.addFilter(filter);
			}

			long t1 = System.currentTimeMillis();
			Scan scan = new Scan();
			scan.setFilter(list);
			ResultScanner rs = table.getScanner(scan);
			for (Result rr = rs.next(); rr != null; rr = rs.next()) {
				byte[] keyBytes = rr.getRow();

				System.out.println(System.currentTimeMillis() - t1 + " ms for " + new String(keyBytes));
			}
			
			totalTime=totalTime+(System.currentTimeMillis() - t1);
		}
		System.out.println("Average "+(totalTime/100)+ " ms for each query");
		
		table.close();
	}

}
