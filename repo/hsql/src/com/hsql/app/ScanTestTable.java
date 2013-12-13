package com.hsql.app;

import java.io.IOException;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

public class ScanTestTable {

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		HTable table=new HTable("test");

		long t1=System.currentTimeMillis();
		Scan scan=new Scan();
		ResultScanner rs=table.getScanner(scan);
		int i=0;
		for (Result rr = rs.next(); rr != null; rr = rs.next()) {
			byte[] res = rr.getRow();
			i++;
			if(i%10000 ==0)
				System.out.println(i);
		}
		System.out.println(System.currentTimeMillis()-t1+" ms for "+i+ "rows");

		table.close();
	}

}
