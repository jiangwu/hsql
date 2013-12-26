package com.hsql.core;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeSet;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;


/**
 * 
 * 
 * This class is not thread-safe.
 */
class UserIndexTable {

	private HTable userHTable = null;
	private HTable indexHTable = null;
	private TreeSet<String> indexNames = new TreeSet<String>();
	private String tableName;
	private String indexTableName;

	public UserIndexTable(String tableName) {
		this.tableName = tableName;
		this.indexTableName = tableName + ".Index";
	}

	/**
	 * open connection to both user table and index table
	 * 
	 * @throws IOException
	 */

	public void open() throws Exception {
		IndexAdminImpl adminUtil = new IndexAdminImpl();
		adminUtil.open();

		String[] cols = adminUtil.getIndexCols(tableName);

		adminUtil.close();
		if (cols != null) {
			for (String s : cols) {
				indexNames.add(s);
			}
		} else {
			throw new Exception("cannot get index information for table "
					+ tableName);
		}

		userHTable = new HTable(tableName);
		indexHTable = new HTable(indexTableName);

	}

	/**
	 * close both user table and index table
	 */

	public void close() {
		try {
			userHTable.close();

		} catch (IOException e) {

		}
		try {
			indexHTable.close();
		} catch (IOException e) {

		}
	}


	public void delete(String pk) throws Exception {
		Get get = new Get(pk.getBytes());
		Result rs = userHTable.get(get);

		if(rs==null){
			throw new Exception("no row found for "+tableName+" pk");
		}

		Map<String, String> indexes = new HashMap<String, String>();
		NavigableMap<byte[], NavigableMap<byte[], byte[]>> fMap = rs
				.getNoVersionMap();
		for (byte[] f : fMap.keySet()) {
			String family = new String(f);
			for (byte[] q : fMap.get(f).keySet()) {
				String qq=new String (q);
				String col=family+":"+qq;
				if (indexNames.contains(col)) {
					String v=new String(fMap.get(f).get(q));
					indexes.put(col, v);
				}
			}
		}
		List<String> indexKeys = IndexCreator.getIndexKeys(indexes, pk,
				indexNames);



		List<Delete> deletes = new ArrayList<Delete>();
		for (String k : indexKeys) {
			Delete delete = new Delete(k.getBytes());
			deletes.add(delete);
		}
		indexHTable.delete(deletes);

	}

	/**
	 * insert a row into user table; also build index in the index table all
	 * index columns must have values if a row with the same primary key already
	 * exists, the previous indexes will be deleted
	 * 
	 * @param key
	 *            is unique in a table
	 * @param allCols
	 *            contain all key-value pairs of columns. All index columns must
	 *            be included.
	 * @throws Exception
	 */

	public void insert(String key, Map<String, String> allCols)
			throws Exception {

		Get get = new Get(key.getBytes());
		Result rs = userHTable.get(get);
		if (rs != null && rs.getRow() != null
				&& new String(rs.getRow()).equals(key)) {
			delete(key);
		}

		Map<String, String> indexCol = new HashMap<String, String>();
		Map<String, String> noneIndexCol = new HashMap<String, String>();

		for (String k : allCols.keySet()) {
			if (indexNames.contains(k)) {
				indexCol.put(k, allCols.get(k));
			} else {
				noneIndexCol.put(k, allCols.get(k));
			}
		}

		if (indexCol.size() < indexNames.size()) {
			throw new Exception("All index columns must have values");
		}


		insertIndex(key, indexCol);
	}

	private void insertIndex(String key, Map<String, String> indexCol)
			throws Exception {
		List<String> indexes = IndexCreator.getIndexKeys(indexCol, key,
				indexNames);

		List<Put> puts = new ArrayList<Put>();
		for (String index : indexes) {

			Put put = new Put((index).getBytes());
			put.add(Constants.userIndexColFamily, Constants.userIndexColqualifier,
					key.getBytes());
			puts.add(put);
		}
		indexHTable.put(puts);
	}

	public void insert(String key, String colValues) throws Exception {
		Map<String, String> cols = new HashMap<String, String>();
		String ss[] = colValues.split(" ");
		for (String s : ss) {
			String[] kv = s.split("=");
			cols.put(kv[0].trim(), kv[1].trim());
		}
		insert(key, cols);

	}

}