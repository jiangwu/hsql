package com.hsql.core;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.junit.Test;

public class Admin {

	private final static String indexMetaTable = "indexMeta";
	private final static byte[] family = "f1".getBytes();
	private final static byte[] qualifier = "indexes".getBytes();

	/**
	 * 
	 * @param tableName
	 * @return the table columns that are indexed
	 * @throws IOException
	 */
	public String[] getIndexCols(String tableName) throws IOException {
		HTable indexTable = new HTable(indexMetaTable);
		Get get = new Get(tableName.getBytes());
		get.addColumn(family, qualifier);

		Result rs = indexTable.get(get);
		indexTable.close();

		if (rs != null) {
			byte[] resB = rs.getValue(family, qualifier);
			if (resB != null && resB.length != 0) {
				String[] res = (new String(resB)).split(",");
				return res;
			} else {
				return null;
			}
		} else {
			return null;
		}
	}

	/**
	 * Create both a table and its index table
	 * 
	 * @param table
	 *            table to be created
	 * @param cols
	 *            columns of the table to be indexed
	 * @throws IOException
	 */
	public void createTable(String table, String[] cols) throws IOException {
		Configuration conf = new Configuration();
		HBaseAdmin admin = new HBaseAdmin(conf);

		// if the indexMeta table is not created, create it first
		if (!admin.isTableAvailable(indexMetaTable)) {
			HTableDescriptor desc = new HTableDescriptor(indexMetaTable);
			HColumnDescriptor cdesc = new HColumnDescriptor(family);
			desc.addFamily(cdesc);
			admin.createTable(desc);
		}

		// create table
		HTableDescriptor desc = new HTableDescriptor(table.getBytes());
		HColumnDescriptor cdesc = new HColumnDescriptor(
				UserTable.userTableFamily);
		desc.addFamily(cdesc);
		admin.createTable(desc);

		// create index table
		desc = new HTableDescriptor(table + "Index");
		cdesc = new HColumnDescriptor(UserTable.indexTableFamily);
		desc.addFamily(cdesc);
		admin.createTable(desc);

		admin.close();

		// write metaTable for the index table

		HTable indexTable = new HTable(indexMetaTable);
		Put put = new Put(table.getBytes());
		StringBuffer sb = new StringBuffer();
		for (String c : cols) {
			sb.append(c);
			sb.append(",");
		}
		put.add(family, qualifier, sb.substring(0, sb.length() - 1).getBytes());
		indexTable.put(put);

		indexTable.close();

	}

	public void deleteTable(String tableName) throws IOException {

		HTable indexTable = new HTable(indexMetaTable);

		Delete delete = new Delete(tableName.getBytes());
		delete.deleteFamily(UserTable.indexTableFamily);

		indexTable.delete(delete);
		indexTable.close();

		Configuration conf = new Configuration();
		HBaseAdmin admin = new HBaseAdmin(conf);
		if (admin.isTableAvailable(tableName)) {
			admin.disableTable(tableName);
			admin.deleteTable(tableName);
		}
		if (admin.isTableAvailable(tableName + "Index")) {
			admin.disableTable(tableName + "Index");
			admin.deleteTable(tableName + "Index");
		}

		admin.close();
	}
	
	public static void main(String[] args){
		if(args.length<2 ){
			System.out.println("Usage:\n -create|-delete tabelName [col1 col2 col3 ...]");
			return;
		}
		String tableName=args[1];
		Admin admin=new Admin();
		
		if(args[0].equals("-create")){

			if(args.length<3){
				System.out.println("to create table, provide column list to be indexed");
				return;
			}
			String[] col=new String [args.length-2];
			for(int i=2;i<args.length;i++){
				col[i-2]=args[i];
			}

			try {
				admin.createTable(tableName, col);
			} catch (IOException e) {
				System.out.println("create table failed.");
				e.printStackTrace();
			}
			
		}else{
			try {
				admin.deleteTable(tableName);
			} catch (IOException e) {
				System.out.println("delete table failed");
				e.printStackTrace();
			}
		}
		
	}
	
	@Test
	public void testMainCreate(){
		main(new String[]{"-create", "testTable", "c0", "c1", "c2"});
	}
	
	@Test
	public void testMainDelete(){
		main(new String[]{"-delete", "testTable"});
	}

	@Test
	public void test2() throws IOException {
		deleteTable("testTable");
	}

	@Test
	public void test1() throws IOException {
		createTable("testTable", new String[] { "c0", "c1", "c2", "c3" });

	}

}
