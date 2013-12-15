package com.hsql.core;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.junit.Test;

public class AdminUtil {

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
				UserTableImpl.userColumnFamily);
		desc.addFamily(cdesc);
		admin.createTable(desc);

		// create index table
		desc = new HTableDescriptor(table + "Index");
		cdesc = new HColumnDescriptor(UserTableImpl.indexColumnFamily);
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
		delete.deleteFamily(UserTableImpl.indexColumnFamily);

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
	public boolean isTableValid(String tableName) throws IOException{
		HBaseAdmin admin=new HBaseAdmin(new Configuration());
		boolean res=true;
		if(!admin.isTableAvailable(tableName) && !admin.isTableAvailable(tableName+"Index")){
			res= false;
		}else{
			res= true;
		}
		admin.close();
		return res;
	}

}
