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
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
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
	// public byte[] userColumnFamily;
	// public final static byte[] indexColumnFamily = "f1".getBytes();
	// public final static byte[] indexQualifier = "primaryKey".getBytes();

	private HTable userHTable = null;
	private HTable indexHTable = null;
	private TreeSet<String> indexNames = new TreeSet<String>();
	private String tableName;
	private String indexTableName;

	public UserTableImpl(String tableName) {
		this.tableName = tableName;
		this.indexTableName = tableName + ".Index";
	}

	/**
	 * open connection to both user table and index table
	 * 
	 * @throws IOException
	 */
	@Override
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
	public void deleteRow(String pk) throws Exception {
		Get get = new Get(pk.getBytes());
		Result rs = userHTable.get(get);
		NavigableMap<byte[], byte[]> kv = rs.getNoVersionMap().values()
				.iterator().next();
		Map<String, String> indexes = new HashMap<String, String>();
		NavigableMap<byte[], NavigableMap<byte[], byte[]>> fMap = rs
				.getNoVersionMap();
		for (byte[] f : fMap.keySet()) {
			String family = new String(f);
			for (byte[] q : fMap.get(f).keySet()) {
				String col=family+":"+new String(q);
				if (indexNames.contains(col)) {
					indexes.put(col, new String(kv.get(q)));
				}
			}
		}
		List<String> indexKeys = IndexCreator.getIndexKeys(indexes, pk,
				indexNames);

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

	private void insert(String key, Map<String, String> allCols)
			throws Exception {

		Get get = new Get(key.getBytes());
		Result rs = userHTable.get(get);
		if (rs != null && rs.getRow() != null
				&& new String(rs.getRow()).equals(key)) {
			deleteRow(key);
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
			String[] fq = e.getKey().split(":");
			String f = fq[0].trim();
			String q = fq[1].trim();
			put.add(f.getBytes(), q.getBytes(), e.getValue().getBytes());
			puts.add(put);
		}
		for (Entry<String, String> e : noneIndexCol.entrySet()) {
			String[] fq = e.getKey().split(":");
			String f = fq[0].trim();
			String q = fq[1].trim();
			put.add(f.getBytes(), q.getBytes(), e.getValue().getBytes());
			puts.add(put);
		}

		userHTable.put(puts);

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
				boolean res = it.hasNext();
				if (res == false) {
					rs.close();
				}
				return res;
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

	private Iterable<UserRow> select(Map<String, String> indexes)
			throws Exception {
		return new RowIterable(indexes);
	}

	private ResultScanner getScanner(Map<String, String> indexes)
			throws Exception {
		String searchKey = IndexCreator.getSearchKey(indexes, indexNames);
		Scan scan = new Scan();
		byte[] startRow = searchKey.getBytes();
		scan.setStartRow(startRow);
		byte[] stopRow = searchKey.getBytes();
		stopRow[stopRow.length - 1]++;
		scan.setStopRow(stopRow);
		scan.addColumn(Constants.userIndexColFamily, Constants.userIndexColqualifier);
		ResultScanner rs = indexHTable.getScanner(scan);
		return rs;
	}

	private UserRow getRow(Result rr) throws IOException {
		String primaryKey = new String(rr.getValue(Constants.userIndexColFamily,
				Constants.userIndexColqualifier));
		Get get = new Get(primaryKey.getBytes());
		Result getRes = userHTable.get(get);
		NavigableMap<byte[], NavigableMap<byte[], byte[]>> resMap = getRes
				.getNoVersionMap();
		// NavigableMap<byte[], byte[]> fMap = resMap.get(userColumnFamily);

		Map<String, String> indexedCol = new HashMap<String, String>();
		Map<String, String> unIndexedCol = new HashMap<String, String>();
		for (byte[] f : resMap.keySet()) {
			NavigableMap<byte[], byte[]> fMap = resMap.get(f);
			for (byte[] kk : fMap.keySet()) {
				String q = new String(kk);
				String v = new String(fMap.get(kk));
				String col = new String(f) + ":" + q;
				if (indexNames.contains(col)) {
					indexedCol.put(col, v);
				} else {
					unIndexedCol.put(col, v);
				}
			}
		}

		UserRow row = new UserRow();
		row.setIndexedCols(indexedCol);
		row.setNonIndexedCols(unIndexedCol);
		row.setKey(primaryKey);
		return row;
	}

	class ORIterable implements Iterable<UserRow> {

		private List<Map<String, String>> indexBlocks;

		public ORIterable(List<Map<String, String>> indexBlocks) {
			this.indexBlocks = indexBlocks;
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
			Iterator<UserRow> currentRowIterator;
			List<Map<String, String>> usedIndexes = new ArrayList<Map<String, String>>();
			private UserRow row4next;
			private int currentIndexSeq;

			public ORIterator() throws Exception {
				currentRowIterator = new RowIterable(indexBlocks.get(0))
						.iterator();
				currentIndexSeq = 0;
			}

			private boolean alreadyGet(UserRow row) {
				for (Map<String, String> indexes : usedIndexes) {
					boolean allMatch = true;
					for (Entry<String, String> e : indexes.entrySet()) {
						if (!row.getIndexedCols().get(e.getKey())
								.equals(e.getValue())) {
							allMatch = false;
						}
					}
					if (allMatch == true) {
						return true;
					}
				}
				return false;
			}

			@Override
			public boolean hasNext() {
				while (currentRowIterator.hasNext()) {
					row4next = currentRowIterator.next();
					if (alreadyGet(row4next)) {
						continue;
					} else {
						return true;
					}
				}
				usedIndexes.add(indexBlocks.get(currentIndexSeq));
				currentIndexSeq++;

				if (currentIndexSeq >= indexBlocks.size()) {
					row4next = null;
					return false;
				} else {
					try {
						currentRowIterator = new RowIterable(
								indexBlocks.get(currentIndexSeq)).iterator();
					} catch (Exception e) {
						throw new RuntimeException(e);
					}
					return hasNext();
				}
			}

			@Override
			public UserRow next() {
				return row4next;

			}

			@Override
			public void remove() {

			}

		}
	}

	private Iterable<UserRow> select(List<Map<String, String>> indexBlocks)
			throws Exception {

		return new ORIterable(indexBlocks);
	}

	List<Map<String, String>> parse(String command) throws Exception {
		try {
			List<Map<String, String>> res = new ArrayList<Map<String, String>>();
			String[] andCommands = command.split("or");
			for (String s : andCommands) {
				Map<String, String> block = new HashMap<String, String>();
				String[] indexes = s.split("and");
				for (String index : indexes) {
					String[] kv = index.split("=");
					block.put(kv[0].trim(), kv[1].trim());
				}
				res.add(block);
			}
			return res;
		} catch (Exception e) {
			throw new Exception("cannot parse command " + command, e);
		}
	}

	@Override
	public Iterable<UserRow> select(String condition) throws Exception {
		List<Map<String, String>> blocks = parse(condition);
		for (Map<String, String> block : blocks) {
			if (!indexNames.containsAll(block.keySet()))
				throw new Exception(condition + "contains invalid index column");
		}
		return select(blocks);
	}

	@Override
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