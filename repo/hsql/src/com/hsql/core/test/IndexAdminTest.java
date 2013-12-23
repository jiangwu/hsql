package com.hsql.core.test;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.hsql.core.IndexAdmin;
import com.hsql.core.IndexAdminImpl;
import com.hsql.core.UserRow;
import com.hsql.core.UserTable;
import com.hsql.core.UserTableFactory;

public class IndexAdminTest {
	static String tableName="testTable";
	static String [] indexes=new String[]{"f1:c1", "f1:c2", "f1:c3"};
	static HBaseAdmin hbaseAdmin=null;
	static IndexAdmin indexAdmin=null;
	
	@BeforeClass
	static public void init() throws IOException{
		hbaseAdmin=new HBaseAdmin(new Configuration());
		indexAdmin=new IndexAdminImpl();
		indexAdmin.open();
		
		if(hbaseAdmin.isTableAvailable(tableName)){
			HTableDescriptor desc = hbaseAdmin.getTableDescriptor(tableName.getBytes());
			hbaseAdmin.disableTable(tableName);
			hbaseAdmin.deleteTable(tableName);
			hbaseAdmin.createTable(desc);
		}else{
			HTableDescriptor desc=new HTableDescriptor(tableName);
			HColumnDescriptor cdesc=new HColumnDescriptor("f1");
			desc.addFamily(cdesc);
			hbaseAdmin.createTable(desc);
		}
		
		HTable table=new HTable(tableName);
		Put put =new Put("1".getBytes());
		put.add("f1".getBytes(), "c1".getBytes(), "v1".getBytes());
		put.add("f1".getBytes(), "c2".getBytes(), "v2".getBytes());
		put.add("f1".getBytes(), "c3".getBytes(), "v3".getBytes());
		table.put(put);
		
		put =new Put("2".getBytes());
		put.add("f1".getBytes(), "c1".getBytes(), "vv1".getBytes());
		put.add("f1".getBytes(), "c2".getBytes(), "vv2".getBytes());
		put.add("f1".getBytes(), "c3".getBytes(), "vv3".getBytes());
		table.put(put);
		table.close();
		
	}
	
	@AfterClass
	static public void cleanup() throws IOException{
		hbaseAdmin.disableTable(tableName);
		hbaseAdmin.deleteTable(tableName);
		hbaseAdmin.close();
		indexAdmin.close();
	}

	@Test
	public void test() throws Exception{
//		assertTrue(indexAdmin.isTableIndexed(tableName)==false);
		indexAdmin.buildIndex(tableName, indexes);
		assertTrue(indexAdmin.isTableIndexed(tableName));
		assertTrue(indexAdmin.getIndexedTables().containsKey(tableName));
		assertTrue(indexAdmin.getIndexedTables().get(tableName).equals("f1:c1,f1:c2,f1:c3"));

		HTable table=new HTable(tableName+".Index");
		ResultScanner rs= table.getScanner("f1".getBytes());
		int i=0;
		for(Result r:rs){
			i++;
		}
		assertTrue(i==8);
		table.close();
		
		
		indexAdmin.deleteIndex(tableName);
		assertTrue(indexAdmin.isTableIndexed(tableName)==false);
		indexAdmin.close();
		
	}

}
