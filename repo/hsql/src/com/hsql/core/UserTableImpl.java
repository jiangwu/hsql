package com.hsql.core;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.junit.Test;

class UserTableImpl implements UserTable{
	public final static byte[] userColumnFamily = "f1".getBytes();
	public final static byte[] indexColumnFamily = "f1".getBytes();
	public final static byte[] indexQualifier = "primaryKey".getBytes();

	HTable userHTable = null;
	HTable indexHTable = null;
	private TreeSet<String> indexNames = new TreeSet<String>();
	private String tableName;

	public UserTableImpl(String tableName) {
		this.tableName=tableName;
	}

	/**
	 * open connection to both user table and index table
	 * 
	 * @throws IOException
	 */
	@Override
	public void open() throws IOException {
		AdminUtil metaTable = new AdminUtil();

		String[] cols = metaTable.getIndexCols(tableName);

		if (cols != null) {
			for (String s : cols) {
				indexNames.add(s);
			}
		}

		userHTable = new HTable(tableName);
		indexHTable = new HTable(tableName + "Index");

	}

	/**
	 * close both user table and index table
	 */
	@Override
	public void close() {
		try {
			userHTable.close();

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {
			indexHTable.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}


	@Override
	public void delete(String pk) throws IOException{
		Get get=new Get(pk.getBytes());
		Result rs=userHTable.get(get);
		NavigableMap<byte[], byte[]> kv = rs.getNoVersionMap().values().iterator().next();
		Map<String, String> indexes=new HashMap<String, String>();
		for(byte[] col:kv.keySet()){
			if(indexNames.contains(new String(col))){
				indexes.put(new String(col), new String(kv.get(col)));
			}
		}
		List<String> indexKeys = IndexCreator.getIndexKeys(indexes, pk);
		
		Delete delete=new Delete(pk.getBytes());
		userHTable.delete(delete);
		
		List<Delete> deletes=new ArrayList<Delete>();
		for(String k:indexKeys){
			delete=new Delete(k.getBytes());
			deletes.add(delete);
		}
		indexHTable.delete(deletes);
		
		//get row, compute index, delete index, delete row
	}

	/**
	 * insert a row into user table; also build index in the index table
	 * all index columns must have values
	 * if a row with the same primary key already exists, the previous indexes will be deleted
	 * 
	 * @param key
	 *            is unique in a table
	 * @param indexCol
	 * @param noneIndexCol
	 * @throws Exception
	 */
	@Override
	public void insert(String key, Map<String, String> allCols)
			throws Exception {
		
		Get get=new Get(key.getBytes());
		Result rs = userHTable.get(get);
		if(rs!=null && rs.getRow()!=null && new String(rs.getRow()).equals(key)){
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

		if(indexCol.size()<indexNames.size()){
			throw new Exception("All index columns must have values");
		}

		Put put = new Put(key.getBytes());
		List<Put> puts = new ArrayList<Put>();

		for (Entry<String, String> e : indexCol.entrySet()) {
			put.add(userColumnFamily, e.getKey().getBytes(), e.getValue()
					.getBytes());
			puts.add(put);
		}
		for (Entry<String, String> e : noneIndexCol.entrySet()) {
			put.add(userColumnFamily, e.getKey().getBytes(), e.getValue()
					.getBytes());
			puts.add(put);
		}

		userHTable.put(puts);

		List<String> indexes = IndexCreator.getIndexKeys(indexCol, key);

		puts.clear();
		for (String index : indexes) {
			put = new Put((index).getBytes());
			put.add(indexColumnFamily, indexQualifier, key.getBytes());
			puts.add(put);
		}
		indexHTable.put(puts);

	}

	class RowIterable implements Iterable<UserRow> {
		Map<String, String> indexes;
		ResultScanner rs;
		private Iterator<Result> it;

		public RowIterable(Map<String, String> indexes) throws Exception {
			if(!indexNames.containsAll(indexes.keySet())){
				throw new Exception("searched columns are not indexed");
			}

			this.indexes = indexes;
		}

		@Override
		public Iterator<UserRow> iterator() {
			try {
				rs = getScanner(indexes);
				it = rs.iterator();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				return null;
			}

			return new RowIterator();
		}

		class RowIterator implements Iterator<UserRow> {

			@Override
			public boolean hasNext() {
				if (it.hasNext() == false) {
					rs.close();
				}
				return it.hasNext();
			}

			@Override
			public UserRow next() {
				Result rr = it.next();
				try {
					return getRow(rr);
				} catch (IOException e) {
					return null;
				}
			}

			@Override
			public void remove() {

			}
		}
	}

	@Override
	public Iterable<UserRow> select(Map<String, String> indexes) throws Exception {
		return new RowIterable(indexes);
	}

	private ResultScanner getScanner(Map<String, String> indexes)
			throws Exception {

		TreeMap<String, String> sortedIndexes = new TreeMap<String, String>();
		sortedIndexes.putAll(indexes);

		StringBuffer key = new StringBuffer();
		int count = 0;
		for (String col : sortedIndexes.keySet()) {
			key.append(col);
			key.append("=");
			key.append(sortedIndexes.get(col));
			key.append("|");
			count++;
		}
		String searchKey;
		int prefix;
		boolean containLast = false;
		// if one col is the last col in all indexed cols
		if (indexNames.descendingSet().first()
				.equals(sortedIndexes.descendingKeySet().first())) {
			containLast = true;
		}

		if (containLast)
			prefix = indexNames.size() - count;
		else
			prefix = indexNames.size() - count - 1;

		searchKey = prefix + "_" + key.substring(0, key.length() - 1);

		Scan scan = new Scan();
		byte[] startRow = searchKey.getBytes();
		scan.setStartRow(startRow);
		byte[] stopRow = searchKey.getBytes();
		stopRow[stopRow.length - 1]++;
		scan.setStopRow(stopRow);
		scan.addColumn(indexColumnFamily, indexQualifier);
		ResultScanner rs = indexHTable.getScanner(scan);
		return rs;
	}

	private UserRow getRow(Result rr) throws IOException {
		String primaryKey = new String(rr.getValue(indexColumnFamily,
				indexQualifier));
		Get get = new Get(primaryKey.getBytes());
		Result getRes = userHTable.get(get);
		NavigableMap<byte[], NavigableMap<byte[], byte[]>> resMap = getRes
				.getNoVersionMap();
		NavigableMap<byte[], byte[]> fMap = resMap.get(userColumnFamily);

		Map<String, String> indexedCol = new HashMap<String, String>();
		Map<String, String> unIndexedCol = new HashMap<String, String>();
		for (byte[] kk : fMap.keySet()) {
			String col = new String(kk);
			String v = new String(fMap.get(kk));
			if (indexNames.contains(col)) {
				indexedCol.put(col, v);
			} else {
				unIndexedCol.put(col, v);
			}
		}

		UserRow row = new UserRow();
		row.setIndexedCols(indexedCol);
		row.setNonIndexedCols(unIndexedCol);
		row.setKey(primaryKey);
		return row;
	}
	


}
