package com.hsql.core;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.TreeSet;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

import com.hsql.core.UserTableImpl.RowIterable.RowIterator;

/**
 * 
 * 
 * This class is not thread-safe.
 */
class UserTableImpl implements UserTable {
	public final static byte[] userColumnFamily = "f1".getBytes();
	public final static byte[] indexColumnFamily = "f1".getBytes();
	public final static byte[] indexQualifier = "primaryKey".getBytes();

	private HTable userHTable = null;
	private HTable indexHTable = null;
	private TreeSet<String> indexNames = new TreeSet<String>();
	private String tableName;

	public UserTableImpl(String tableName) {
		this.tableName = tableName;
	}

	/**
	 * open connection to both user table and index table
	 * 
	 * @throws IOException
	 */
	@Override
	public void open() throws Exception {
		AdminUtil adminUtil = new AdminUtil();

		String[] cols = adminUtil.getIndexCols(tableName);

		if (cols != null) {
			for (String s : cols) {
				indexNames.add(s);
			}
		} else {
			throw new Exception("cannot get index information for table "
					+ tableName);
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

		}
		try {
			indexHTable.close();
		} catch (IOException e) {

		}
	}

	@Override
	public void delete(String pk) throws Exception {
		Get get = new Get(pk.getBytes());
		Result rs = userHTable.get(get);
		NavigableMap<byte[], byte[]> kv = rs.getNoVersionMap().values()
				.iterator().next();
		Map<String, String> indexes = new HashMap<String, String>();
		for (byte[] col : kv.keySet()) {
			if (indexNames.contains(new String(col))) {
				indexes.put(new String(col), new String(kv.get(col)));
			}
		}
		List<String> indexKeys = IndexCreator.getIndexKeys(indexes, pk, indexNames);

		Delete delete = new Delete(pk.getBytes());
		userHTable.delete(delete);

		List<Delete> deletes = new ArrayList<Delete>();
		for (String k : indexKeys) {
			delete = new Delete(k.getBytes());
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
	@Override
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

		List<String> indexes = IndexCreator.getIndexKeys(indexCol, key, indexNames);

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
			if (!indexNames.containsAll(indexes.keySet())) {
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
				boolean res=it.hasNext() == false;
				if (res==false) {
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
	public Iterable<UserRow> select(Map<String, String> indexes)
			throws Exception {
		return new RowIterable(indexes);
	}

	private ResultScanner getScanner(Map<String, String> indexes)
			throws Exception {
		String searchKey=IndexCreator.getSearchKey(indexes, indexNames);
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
	
	class ORIterable implements Iterable<UserRow>{

		private List<Map<String, String>> indexBlocks;
		public ORIterable(List<Map<String, String>> indexBlocks){
			this.indexBlocks=indexBlocks;
		}
		@Override
		public Iterator<UserRow> iterator() {

			try {
				return new ORIterator();
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
		class ORIterator implements Iterator<UserRow> {
			Map<String,String> currentIndex;
			RowIterable currentRowIterable;
			Iterator<UserRow> currentRowIterator;
			List<Map<String, String>> usedIndexes=new ArrayList<Map<String,String>>();
			
			public ORIterator() throws Exception{
				RowIterable currentRowIterable=new RowIterable(indexBlocks.get(0));
				currentRowIterator=currentRowIterable.iterator();
				currentIndex=indexBlocks.get(0);
			}

			@Override
			public boolean hasNext() {
					if(usedIndexes.size()==0){
						if(currentRowIterator.hasNext()==true){
							return true;
						}else{
							usedIndexes.add(currentIndex);
							currentIndex=indexBlocks.get(usedIndexes.size());
							try {
								currentRowIterable=new RowIterable(currentIndex);
							} catch (Exception e) {
								throw new RuntimeException(e);
							}
							currentRowIterator=currentRowIterable.iterator();
							while()
						}
					}
			}

			@Override
			public UserRow next() {
				if(usedIndexes.size()==0){
					return currentRowIterator.next();
				}
				

			}

			@Override
			public void remove() {

				
			}
			
		}
	}

	@Override
	public Iterable<UserRow> select(List<Map<String, String>> indexBlocks)
			throws Exception {
		List<Map<String, String>> usedConditions=new ArrayList<Map<String, String>>();
		int i=0;
		for(Map<String,String> indexes: indexBlocks){
			
		}
		return null;
	}

}
