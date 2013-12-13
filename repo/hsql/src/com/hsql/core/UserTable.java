package com.hsql.core;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.junit.Test;

public class UserTable {
	public final static byte[] userTableFamily = "f1".getBytes();
	public final static byte[] indexTableFamily = "f1".getBytes();
	public final static byte[] indexTableCol = "primaryKey".getBytes();

	HTable table = null;
	HTable indexTable = null;
	private TreeSet<String> indexColNames = new TreeSet<String>();



	/**
	 * open connection to both user table and index table
	 * 
	 * @throws IOException
	 */
	public void open(String tableName) throws IOException {
		Admin metaTable = new Admin();

		String[] cols = metaTable.getIndexCols(tableName);

		if (cols != null) {
			for (String s : cols) {
				indexColNames.add(s);
			}
		}

		table = new HTable(tableName);
		indexTable = new HTable(tableName + "Index");

	}

	public void close() {
		try {
			table.close();

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {
			indexTable.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * verify if the column names are valid
	 * 
	 * @param indexedCol
	 * @throws Exception
	 */
	private void validate(Map<String, String> indexedCol) throws Exception {
		for (String c : indexedCol.keySet()) {
			if (!indexColNames.contains(c))
				throw new Exception("column " + c + " is not indexed");
		}
	}

	/**
	 * insert a row into user table; also build index in the index table
	 * 
	 * @param key
	 *            is unique in a table
	 * @param indexCol
	 * @param noneIndexCol
	 * @throws Exception
	 */
	public void insert(String key, Map<String, String> allCols) throws Exception {

		Map<String, String> indexCol=new HashMap<String, String>();
		Map<String, String> noneIndexCol=new HashMap<String, String>();
		
		for(String k:allCols.keySet()){
			if(indexColNames.contains(k)){
				indexCol.put(k, allCols.get(k));
			}else{
				noneIndexCol.put(k, allCols.get(k));
			}
		}
		

		
		validate(indexCol);

		Put put = new Put(key.getBytes());
		List<Put> puts = new ArrayList<Put>();

		for (Entry<String, String> e : indexCol.entrySet()) {
			put.add(userTableFamily, e.getKey().getBytes(), e.getValue()
					.getBytes());
			puts.add(put);
		}
		for (Entry<String, String> e : noneIndexCol.entrySet()) {
			put.add(userTableFamily, e.getKey().getBytes(), e.getValue()
					.getBytes());
			puts.add(put);
		}

		table.put(puts);

		List<String> indexes = IndexCreator.getIndexKeys(indexCol, key);

		puts.clear();
		for (String index : indexes) {
			put = new Put((index).getBytes());
			put.add(indexTableFamily, indexTableCol, key.getBytes());
			puts.add(put);
		}
		indexTable.put(puts);

	}

	/**
	 * 
	 * @param indexes
	 *            column=>value pairs to query. column must be used in index
	 * @return the index columns
	 * @throws Exception
	 */
	public List<UserRow> query(Map<String, String> indexes) throws Exception {
		validate(indexes);

		TreeMap<String, String> sortedIndexes = new TreeMap<String, String>();
		sortedIndexes.putAll(indexes);


		List<UserRow> res = new ArrayList<UserRow>();
		StringBuffer key = new StringBuffer();
		int count = 0;
		for (String col:sortedIndexes.keySet()) {
			key.append(col);
			key.append("=");
			key.append(sortedIndexes.get(col));
			key.append("|");
			count++;
		}
		String searchKey;
		int prefix;
		boolean containLast = false;
		//if one col is the last col in all indexed cols
		if(indexColNames.descendingSet().first().equals(sortedIndexes.descendingKeySet().first())){
			containLast = true;
		}
		{
			if (containLast)
				prefix = indexColNames.size() - count;
			else
				prefix = indexColNames.size() - count - 1;
		}

		searchKey = prefix + "_" + key.substring(0, key.length() - 1);

		Scan scan = new Scan();
		byte[] startRow = searchKey.getBytes();
		scan.setStartRow(startRow);
		byte[] stopRow = searchKey.getBytes();
		stopRow[stopRow.length - 1]++;
		scan.setStopRow(stopRow);
		scan.addColumn(indexTableFamily, indexTableCol);
		ResultScanner rs = null;
		String primaryKey = null;
		try {
			rs = indexTable.getScanner(scan);
			for (Result rr = rs.next(); rr != null; rr = rs.next()) {
				Map<String, String> indexedCol = new HashMap<String, String>();
				Map<String, String> unIndexedCol = new HashMap<String, String>();
				primaryKey = new String(rr.getValue(indexTableFamily,
						indexTableCol));
				Get get = new Get(primaryKey.getBytes());
				// for (String ccol : indexColNames) {
				// get.addColumn(userTableFamily, ccol.getBytes());
				// }
				Result getRes = table.get(get);
				NavigableMap<byte[], NavigableMap<byte[], byte[]>> resMap = getRes
						.getNoVersionMap();
				NavigableMap<byte[], byte[]> fMap = resMap.get(userTableFamily);

				for (byte[] kk : fMap.keySet()) {
					String col = new String(kk);
					String v = new String(fMap.get(kk));
					if (indexColNames.contains(col)) {
						indexedCol.put(col, v);
					} else {
						unIndexedCol.put(col, v);
					}
				}

				UserRow row = new UserRow();
				row.setIndexedCols(indexedCol);
				row.setNonIndexedCols(unIndexedCol);
				row.setKey(primaryKey);
				res.add(row);

			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			rs.close();
		}

		return res;

	}

	@Test(expected = Exception.class)
	public void invalidInsertTest() throws Exception {
		try {
			open("testTable");
		} catch (Exception e) {
			e.printStackTrace();
			return;
		}

		try {
			String primaryKey = "1";
			Map<String, String> indexCol = new HashMap<String, String>();
			for (String col : new String[] { "cc0", "c1", "c2", "c3" }) {
				indexCol.put(col, "v" + col);
			}
			insert(primaryKey, indexCol);
		} finally {
			close();
		}
	}

	@Test
	public void insertTest() {

		try {
			open("testTable");
		} catch (Exception e) {
			e.printStackTrace();
			return;
		}

		String primaryKey = "1";
		Map<String, String> indexCol = new HashMap<String, String>();
		for (String col : new String[] { "c0", "c1", "c2", "c3" }) {
			indexCol.put(col, "v" + col);
		}

		try {
			insert(primaryKey, indexCol);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {

			close();
		}
	}

	@Test
	public void queryTest() throws Exception {

		try {
			open("testTable");
		} catch (Exception e) {
			e.printStackTrace();
			return;
		}

		// String primaryKey = "1";
		// Map<String, String> indexCol = new HashMap<String, String>();
		// for (String col : new String[] { "c0", "c1", "c2", "c3" }) {
		// indexCol.put(col, "v" + col);
		// }
		//
		// insert(primaryKey, indexCol, new HashMap<String, String>());

		Map<String, String> map = new HashMap<String, String>();
		map.put("c0", "vc0");

		List<UserRow> rows = query(map);
		for (UserRow row : rows) {
			Map<String, String> colValue = row.getIndexedCols();
			for (String col : colValue.keySet()) {
				System.out.print(col + "=" + colValue.get(col) + ",");
			}
			System.out.println();
		}
		close();
	}

}
