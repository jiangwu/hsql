package com.hsql.core;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

public class IndexAdminImpl implements IndexAdmin{
	HTable indexTable=null;
	@Override 
	public void open() throws IOException{
		indexTable = new HTable(Constants.metaTable);
	}
	@Override
	public void close() throws IOException{
		if(indexTable!=null)
			indexTable.close();
	}

	/**
	 * 
	 * @param tableName
	 * @return the table columns that are indexed
	 * @throws IOException
	 */
	@Override
	public String[] getIndexCols(String tableName) throws IOException {
		Get get = new Get(tableName.getBytes());
		get.addColumn(Constants.metaColFamily, Constants.metaColqualifier);

		Result rs = indexTable.get(get);


		if (rs != null) {
			byte[] resB = rs.getValue(Constants.metaColFamily,
					Constants.metaColqualifier);
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

	private void insertIndex(HTable indexHTable, String key,
			Map<String, String> indexCol, String[] indexNames) throws Exception {
		TreeSet<String> allIndex = new TreeSet<String>();
		for (String s : indexNames) {
			allIndex.add(s);
		}
		List<String> indexes = IndexCreator.getIndexKeys(indexCol, key,
				allIndex);

		List<Put> puts = new ArrayList<Put>();
		for (String index : indexes) {

			Put put = new Put((index).getBytes());
			put.add(Constants.userIndexColFamily,
					Constants.userIndexColqualifier, key.getBytes());
			puts.add(put);
		}
		indexHTable.put(puts);
	}

	private void computeIndex(String userTableName,
			String userTableIndexFamily, String[] indexNames) throws Exception {

		Scan scan = new Scan();
		for (String q : indexNames) {
			scan.addColumn(userTableIndexFamily.getBytes(), q.getBytes());
		}

		HTable userHTable = new HTable(userTableName);
		HTable userIndexTable = new HTable(userTableName + ".Index");
		ResultScanner rs = userHTable.getScanner(scan);
		Map<String, String> indexCols = new HashMap<String, String>();

		for (Result res : rs) {

			indexCols.clear();
			String key = new String(res.getRow());
			Collection<NavigableMap<byte[], byte[]>> maps = res
					.getNoVersionMap().values();
			for (NavigableMap<byte[], byte[]> map : maps) {
				for (Entry<byte[], byte[]> e : map.entrySet()) {
					String qualifier = new String(e.getKey());
					String value = new String(e.getValue());
					indexCols.put(qualifier, value);

				}
			}
			insertIndex(userIndexTable, key, indexCols, indexNames);
		}
		rs.close();
		userHTable.close();

		// System.out.println("======in count "+inCount);
		// System.out.println("======out count "+outCount);

	}

	@Override
	public void buildIndex(String table, String[] cols) throws Exception {

		String userTableIndexFamily = cols[0].split(":")[0];

		for (String col : cols) {
			if (!col.split(":")[0].equals(userTableIndexFamily)) {
				throw new Exception("Indexes are not in the same column family");
			}
		}

		Configuration conf = new Configuration();
		HBaseAdmin admin = new HBaseAdmin(conf);

		// if the indexMeta table is not created, create it first
		if (!admin.isTableAvailable(Constants.metaTable)) {
			HTableDescriptor desc = new HTableDescriptor(Constants.metaTable);
			HColumnDescriptor cdesc = new HColumnDescriptor(
					Constants.metaColFamily);
			desc.addFamily(cdesc);
			admin.createTable(desc);
		}

		// create index table, or purge the table
		String indexTableName = table + ".Index";
		if (!admin.isTableAvailable(indexTableName)) {
			HTableDescriptor desc = new HTableDescriptor(indexTableName);
			HColumnDescriptor cdesc = new HColumnDescriptor(
					UserTableImpl.indexColumnFamily);
			desc.addFamily(cdesc);
			admin.createTable(desc);
		} else {
			HTableDescriptor desc = admin.getTableDescriptor(indexTableName
					.getBytes());
			admin.disableTable(indexTableName);
			admin.deleteTable(indexTableName);
			admin.createTable(desc);
		}

		admin.close();

		// write metaTable for the index table

		Put put = new Put(table.getBytes());
		StringBuffer sb = new StringBuffer();
		for (String c : cols) {
			sb.append(c);
			sb.append(",");
		}
		put.add(Constants.metaColFamily, Constants.metaColqualifier, sb
				.substring(0, sb.length() - 1).getBytes());
		indexTable.put(put);

		computeIndex(table, userTableIndexFamily, cols);

	}

	@Override
	public void deleteIndex(String tableName) throws IOException {

		Delete delete = new Delete(tableName.getBytes());
		delete.deleteFamily(Constants.userIndexColFamily);

		indexTable.delete(delete);

		Configuration conf = new Configuration();
		HBaseAdmin admin = new HBaseAdmin(conf);

		if (admin.isTableAvailable(tableName + ".Index")) {
			admin.disableTable(tableName + ".Index");
			admin.deleteTable(tableName + ".Index");
		}

		admin.close();
	}

	@Override
	public boolean isTableIndexed(String tableName) throws IOException {
		HBaseAdmin admin = new HBaseAdmin(new Configuration());
		boolean res = true;
		if (!admin.isTableAvailable(tableName)
				&& !admin.isTableAvailable(tableName + ".Index")) {
			res = false;
		} else {
			res = true;
		}
		admin.close();
		return res && getIndexCols(tableName) != null;
	}

	@Override
	public Map<String, String> getIndexedTables() throws IOException {
		Map<String,String> res=new TreeMap<String,String>();
		ResultScanner rs = indexTable.getScanner(Constants.metaColFamily, Constants.metaColqualifier);
		for(Result r: rs){
			String table=new String(r.getRow());
			String indexes=new String(r.getColumnLatest(Constants.metaColFamily, Constants.metaColqualifier).getValue());
			res.put(table, indexes);
		}
		rs.close();
		return res;
	}

}
