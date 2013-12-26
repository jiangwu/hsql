package com.hsql.core.test;

import static org.junit.Assert.*;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class IndexCoprocessorTest {
	
	private static HTable t;
	private static HTable index;

	@BeforeClass
	public static void init() throws IOException{
		t=new HTable("test");
		index=new HTable("test.Index");
	}
	@AfterClass 
	public static void cleanup() throws IOException{
		t.close();
		index.close();
	}

	@Test
	public void testInsert() throws IOException{
		
		Put put=new Put("1".getBytes());
		put.add("f1".getBytes(), "c1".getBytes(), "v1".getBytes());
		put.add("f1".getBytes(), "c2".getBytes(), "v2".getBytes());
		t.put(put);
		
		ResultScanner rs = index.getScanner(new Scan());
		int i=0;
		for(Result r:rs){
			i++;
		}
		rs.close();
		assertTrue(i==2);

	}
	
	@Test
	public void testDelete() throws IOException{

		Delete delete=new Delete("1".getBytes());
		t.delete(delete);
		
		ResultScanner rs = index.getScanner(new Scan());
		int i=0;
		for(Result r:rs){
			i++;
		}
		rs.close();
		assertTrue(i==0);

	}

}
