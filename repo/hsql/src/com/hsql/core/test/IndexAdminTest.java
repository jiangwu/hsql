package com.hsql.core.test;

import static org.junit.Assert.*;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.hsql.core.IndexAdmin;
import com.hsql.core.IndexAdminImpl;

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
		assertTrue(indexAdmin.isTableIndexed(tableName)==false);
		indexAdmin.buildIndex(tableName, indexes);
		assertTrue(indexAdmin.isTableIndexed(tableName));
		assertTrue(indexAdmin.getIndexedTables().containsKey(tableName));
		assertTrue(indexAdmin.getIndexedTables().get(tableName).equals("f1:c1,f1:c2,f1:c3"));

		
		indexAdmin.deleteIndex(tableName);
		assertTrue(indexAdmin.isTableIndexed(tableName)==false);
		indexAdmin.close();
		
	}

}
