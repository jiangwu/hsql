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
	public void test1() throws Exception {
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
		indexes.put("a1", "1");

		assertTrue(table.select(indexes).iterator().next().getKey().equals("1"));

		indexes = new HashMap<String, String>();
		indexes.put("a2", "b");
		assertTrue(table.select(indexes).iterator().next().getKey().equals("2"));

		table.delete("2");
		assertTrue(table.select(indexes).iterator().hasNext() == false);

		cols = new HashMap<String, String>();
		cols.put("a1", "1");
		cols.put("a2", "22");
		cols.put("a3", "3");
		cols.put("a4", "4");
		cols.put("a5", "5");
		cols.put("a6", "6");
		table.insert("3", cols);

		indexes.clear();
		indexes.put("a2", "22");
		assertTrue(table.select(indexes).iterator().next().getKey().equals("3"));

		indexes.clear();
		indexes.put("a3", "3");
		Iterator<UserRow> it = table.select(indexes).iterator();
		Set<String> keys = new HashSet<String>();
		keys.add(it.next().getKey());
		keys.add(it.next().getKey());

		assertTrue(keys.contains("1") && keys.contains("3"));

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
