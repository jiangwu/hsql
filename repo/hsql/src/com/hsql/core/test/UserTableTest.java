package com.hsql.core.test;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.hsql.core.AdminUtil;
import com.hsql.core.UserRow;
import com.hsql.core.UserTable;
import com.hsql.core.UserTableFactory;

public class UserTableTest {
	static AdminUtil admin = null;
	static String tableName = "unitTestTable";

	@BeforeClass
	static public void init() throws IOException {
		admin = new AdminUtil();
		if (admin.isTableValid(tableName)) {
			admin.deleteTable(tableName);
		}
		admin.createTable(tableName, new String[] { "a1", "a2", "a3" });
	}

	@AfterClass
	static public void cleanup() throws IOException {
		admin.deleteTable(tableName);

	}

	@Test
	public void testInsert1Select1() throws Exception {
		UserTable table = UserTableFactory.getTable(tableName);
		table.open();
		Map<String, String> cols = new HashMap<String, String>();
		cols.put("a1", "1");
		cols.put("a2", "2");
		cols.put("a3", "3");
		cols.put("a4", "4");
		cols.put("a5", "5");
		cols.put("a6", "6");
		table.insert("1", cols);

		Map<String, String> indexes = new HashMap<String, String>();
		indexes.put("a1", "1");

		assertTrue(table.select(indexes).iterator().next().getKey().equals("1"));

		indexes.clear();
		indexes.put("a2", "2");

		assertTrue(table.select(indexes).iterator().next().getKey().equals("1"));

		indexes.clear();
		indexes.put("a3", "3");

		assertTrue(table.select(indexes).iterator().next().getKey().equals("1"));

		indexes.clear();
		indexes.put("a1", "1");
		indexes.put("a3", "3");
		assertTrue(table.select(indexes).iterator().next().getKey().equals("1"));

		indexes.clear();
		indexes.put("a2", "2");
		indexes.put("a3", "3");
		assertTrue(table.select(indexes).iterator().next().getKey().equals("1"));

		table.close();
	}
	
	@Test
	public void testInsert2Select1() throws Exception {
		UserTable table = UserTableFactory.getTable(tableName);
		table.open();
		Map<String, String> cols = new HashMap<String, String>();
		cols.put("a1", "1");
		cols.put("a2", "2");
		cols.put("a3", "3");
		cols.put("a4", "4");
		cols.put("a5", "5");
		cols.put("a6", "6");
		table.insert("1", cols);

		cols = new HashMap<String, String>();
		cols.put("a1", "a");
		cols.put("a2", "b");
		cols.put("a3", "c");
		cols.put("a4", "d");
		cols.put("a5", "e");
		cols.put("a6", "f");
		table.insert("2", cols);

		Map<String, String> indexes = new HashMap<String, String>();
		indexes.put("a1", "a");

		assertTrue(table.select(indexes).iterator().next().getKey().equals("2"));

		indexes.clear();
		indexes.put("a2", "b");

		assertTrue(table.select(indexes).iterator().next().getKey().equals("2"));

		indexes.clear();
		indexes.put("a3", "c");

		assertTrue(table.select(indexes).iterator().next().getKey().equals("2"));

		indexes.clear();
		indexes.put("a1", "a");
		indexes.put("a3", "c");
		assertTrue(table.select(indexes).iterator().next().getKey().equals("2"));

		indexes.clear();
		indexes.put("a2", "b");
		indexes.put("a3", "c");
		assertTrue(table.select(indexes).iterator().next().getKey().equals("2"));

		

		table.close();
	}
	
	
	@Test
	public void testInsert2Select2() throws Exception {
		UserTable table = UserTableFactory.getTable(tableName);
		table.open();
		Map<String, String> cols = new HashMap<String, String>();
		cols.put("a1", "1");
		cols.put("a2", "2");
		cols.put("a3", "3");
		cols.put("a4", "4");
		cols.put("a5", "5");
		cols.put("a6", "6");
		table.insert("1", cols);

		cols = new HashMap<String, String>();
		cols.put("a1", "a");
		cols.put("a2", "2");
		cols.put("a3", "3");
		cols.put("a4", "d");
		cols.put("a5", "e");
		cols.put("a6", "f");
		table.insert("2", cols);

		Map<String, String> indexes = new HashMap<String, String>();
		indexes.put("a2", "2");
		indexes.put("a3", "3");

		Iterator<UserRow> it = table.select(indexes).iterator();
		Set<String> keys=new HashSet<String>();
		keys.add(it.next().getKey());
		keys.add(it.next().getKey());
		
		Set<String> expectedKeys=new HashSet<String>();
		expectedKeys.add("1");
		expectedKeys.add("2");
		
		assertTrue(keys.containsAll(expectedKeys) && keys.size()==2);

		table.close();
	}
	
	@Test
	public void testDelete() throws Exception{
		UserTable table = UserTableFactory.getTable(tableName);
		table.open();
		Map<String, String> cols = new HashMap<String, String>();
		cols.put("a1", "1");
		cols.put("a2", "2");
		cols.put("a3", "3");
		cols.put("a4", "4");
		cols.put("a5", "5");
		cols.put("a6", "6");
		table.insert("1", cols);
		
		Map<String, String> indexes = new HashMap<String, String>();
		indexes.put("a1", "1");

		assertTrue(table.select(indexes).iterator().next().getKey().equals("1"));

		
		table.delete("1");


		assertTrue(table.select(indexes).iterator().hasNext()==false);



		table.close();
	}

	@Test(expected = Exception.class)
	public void testInsertLackIndex() throws Exception {
		UserTable table = UserTableFactory.getTable(tableName);
		table.open();
		Map<String, String> cols = new HashMap<String, String>();

		cols.put("a2", "2");
		cols.put("a3", "3");
		cols.put("a4", "4");
		cols.put("a5", "5");
		cols.put("a6", "6");
		table.insert("1", cols);

		table.close();
	}

	@Test(expected = Exception.class)
	public void testSelectWrongIndex() throws Exception {
		UserTable table = UserTableFactory.getTable(tableName);
		table.open();
		Map<String, String> cols = new HashMap<String, String>();

		cols.put("a2", "2");
		cols.put("a3", "3");
		cols.put("a4", "4");

		try {
			table.select(cols);
		} catch (Exception e) {
			table.close();
			throw e;
		}

	}

}
