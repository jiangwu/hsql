package com.hsql.core;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;

// NOTE: 1. need to find if coprocessors should be thread safe. 2. May need to create a pool for performance

public class IndexCoprocessor extends BaseRegionObserver {

	Map<String, UserIndexTable> userIndexTables = new HashMap<String, UserIndexTable>();


	@Override
	public void preDelete(
			ObserverContext<RegionCoprocessorEnvironment> context,
			Delete delete, WALEdit wal, boolean arg3) throws IOException {
		String tableName = context.getEnvironment().getRegion().getRegionInfo()
				.getTableNameAsString();
		String key = new String(delete.getRow());
		UserIndexTable indexTable = getIndexTable(tableName);
		try {
			indexTable.delete(key);
		} catch (Exception e) {
			throw new IOException(e);
		}
	}

	private UserIndexTable getIndexTable(String tableName) throws IOException {
		if (!userIndexTables.keySet().contains(tableName)) {
			UserIndexTable t = new UserIndexTable(tableName);
			try {
				t.open();
			} catch (Exception e) {
				throw new IOException(e);
			}
			userIndexTables.put(tableName, t);
		}
		UserIndexTable indexTable = userIndexTables.get(tableName);
		return indexTable;
	}

	@Override
	public void postPut(ObserverContext<RegionCoprocessorEnvironment> context,
			Put put, WALEdit wal, boolean arg3) throws IOException {

		Map<String, String> allCols = new HashMap<String, String>();

		String tableName = context.getEnvironment().getRegion().getRegionInfo()
				.getTableNameAsString();
		String key = new String(put.getRow());
		
		Map<byte[], List<KeyValue>> fMap = put.getFamilyMap();
		for (byte[] f : fMap.keySet()) {
			String family = new String(f);
			List<KeyValue> kvs = fMap.get(f);
			for (KeyValue kv : kvs) {
			
				kv.getValue();
				String q = new String(kv.getQualifier());
				String v = new String(kv.getValue());
				allCols.put(family + ":" + q, v);
			}
		}

		UserIndexTable indexTable = getIndexTable(tableName);

		try {
			indexTable.insert(key, allCols);
		} catch (Exception e) {
			throw new IOException(e);
		}
	}

}
