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

public class UseCaseTest {
	static AdminUtil admin = null;
	static String tableName = "unitTestTable";

	@BeforeClass
	static public void init() throws Exception {
		admin = new AdminUtil();
		if (admin.isTableValid(tableName)) {
			admin.deleteTable(tableName);
		}
		admin.createTable(tableName, new String[] { "Meta:name", "Meta:dept", "Meta:team" });
	}

	@AfterClass
	static public void cleanup() throws IOException {
		admin.deleteTable(tableName);

	}
	

	@Test
	public void testValueLength() throws Exception {
		UserTable table = UserTableFactory.getTable(tableName);
		table.open();

		table.insert("1", "name=jw dept=credit team=3 a4=4 a5=5 a6=6");
		table.insert("2", "name=sy dept=credit team=3 a4=4 a5=5 a6=6");

		Iterator<UserRow> it = table.select("name=jw").iterator();
		assertTrue(it.hasNext());
		assertTrue(it.next().getKey().equals("1"));
		assertTrue(it.hasNext()==false);
		
		it = table.select("name=sy").iterator();
		assertTrue(it.hasNext());
		assertTrue(it.next().getKey().equals("2"));
		assertTrue(it.hasNext()==false);
		
		it = table.select("name=ad").iterator();
		assertTrue(it.hasNext()==false);
		
		it=table.select("dept=credit").iterator();
		assertTrue(it.hasNext());
		System.out.println(it.next().getKey());
		assertTrue(it.hasNext());
		System.out.println(it.next().getKey());
		assertTrue(it.hasNext()==false);
		
		
		table.deleteRow("1");
		table.deleteRow("2");

		table.close();
	}
	
	@Test
	public void testInsert1Select1() throws Exception {
		UserTable table = UserTableFactory.getTable(tableName);
		table.open();

		table.insert("1", "name=1 dept=2 team=3 a4=4 a5=5 a6=6");

		Iterator<UserRow> it = table.select("name=1").iterator();
		assertTrue(it.hasNext());
		assertTrue(it.next().getKey().equals("1"));

		it = table.select("dept=2").iterator();
		assertTrue(it.hasNext());
		assertTrue(it.next().getKey().equals("1"));
		
		it = table.select("team=3").iterator();
		assertTrue(it.hasNext());
		assertTrue(it.next().getKey().equals("1"));

		it = table.select("name=1 and team=3").iterator();
		assertTrue(it.hasNext());
		assertTrue(it.next().getKey().equals("1"));

		it = table.select("dept=2 and team=3").iterator();
		assertTrue(it.hasNext());
		assertTrue(it.next().getKey().equals("1"));

		table.deleteRow("1");

		table.close();
	}

	@Test
	public void testInsert2Select1() throws Exception {
		UserTable table = UserTableFactory.getTable(tableName);
		table.open();

		table.insert("1", "name=1 dept=2 team=3 a4=4 a5=5 a6=6");


		table.insert("2", "name=a dept=b team=c a4=d a5=e a6=f");
		
		Iterator<UserRow> it = table.select("name=a").iterator();
		assertTrue(it.hasNext());
		assertTrue(it.next().getKey().equals("2"));

		it = table.select("dept=b").iterator();
		assertTrue(it.hasNext());
		assertTrue(it.next().getKey().equals("2"));

		it = table.select("team=c").iterator();
		assertTrue(it.hasNext());
		assertTrue(it.next().getKey().equals("2"));
		
		it = table.select("name=a and team=c").iterator();
		assertTrue(it.hasNext());
		assertTrue(it.next().getKey().equals("2"));

		it = table.select("dept=b and team=c").iterator();
		assertTrue(it.hasNext());
		assertTrue(it.next().getKey().equals("2"));

		table.deleteRow("1");
		table.deleteRow("2");
		table.close();
	}

	@Test
	public void testInsert2Select2() throws Exception {
		UserTable table = UserTableFactory.getTable(tableName);
		table.open();

		table.insert("1", "name=1 dept=2 team=3 a4=4 a5=5 a6=6");


		table.insert("2", "name=a dept=2 team=3 a4=d a5=e a6=f");



		Iterator<UserRow> it = table.select("dept=2 and team=3").iterator();
		Set<String> keys = new HashSet<String>();
		it.hasNext();
		keys.add(it.next().getKey());
		it.hasNext();
		keys.add(it.next().getKey());

		Set<String> expectedKeys = new HashSet<String>();
		expectedKeys.add("1");
		expectedKeys.add("2");

		assertTrue(keys.containsAll(expectedKeys) && keys.size() == 2);

		table.deleteRow("1");
		table.deleteRow("2");
		table.close();
	}

	@Test
	public void testOr() throws Exception {
		UserTable table = UserTableFactory.getTable(tableName);
		table.open();

		table.insert("1", "name=1 dept=2 team=3 a4=4 a5=5 a6=6");


		table.insert("2", "name=a dept=b team=3 a4=d a5=e a6=f");
		

		table.insert("3", "name=a dept=A team=3 a4=d a5=e a6=f");
		

		Iterator<UserRow> it = table.select("dept=2 or dept=b").iterator();
		Set<String> keys = new HashSet<String>();
		while (it.hasNext()) {
			keys.add(it.next().getKey());
		}

		Set<String> expectedKeys = new HashSet<String>();
		expectedKeys.add("1");
		expectedKeys.add("2");

		assertTrue(keys.containsAll(expectedKeys) && keys.size() == 2);

		table.deleteRow("1");
		table.deleteRow("2");
		table.deleteRow("3");

		table.close();
	}

	@Test
	public void testDelete() throws Exception {
		UserTable table = UserTableFactory.getTable(tableName);
		table.open();

		table.insert("1", "name=1 dept=2 team=3 a4=4 a5=5 a6=6");

		Map<String, String> indexes = new HashMap<String, String>();
		indexes.put("name", "1");
		
		Iterator<UserRow> it = table.select("name=1").iterator();
		
		assertTrue(it.hasNext());

		assertTrue(it.next().getKey().equals("1"));

		table.deleteRow("1");
		
		it = table.select("name=1").iterator();
		
		assertTrue(it.hasNext() == false);
		

		table.close();
	}
	
	@Test
	public void testCommandSingle() throws Exception {
		UserTable table = UserTableFactory.getTable(tableName);
		table.open();

		table.insert("1", "name=1 dept=2 team=3 a4=4 a5=5 a6=6");

		Iterator<UserRow> it = table.select("dept=2").iterator();
		assertTrue(it.hasNext()==true);
		assertTrue(it.next().getKey().equals("1"));

		table.deleteRow("1");


		table.close();
	}
	
	@Test
	public void testCommands() throws Exception {
		UserTable table = UserTableFactory.getTable(tableName);
		table.open();

		table.insert("1", "name=1 dept=2 team=3 a4=4 a5=5 a6=6");


		table.insert("2", "name=a dept=b team=3 a4=d a5=e a6=f");
		

		table.insert("3", "name=a dept=A team=B a4=d a5=e a6=f");
		
		Iterator<UserRow> it = table.select("name=a and dept=b").iterator();
		assertTrue(it.hasNext());
		assertTrue(it.next().getKey().equals("2"));
		assertTrue(it.hasNext()==false);
		
		it = table.select("name=a or team=B").iterator();
		Set<String> keys=new HashSet<String>();
		assertTrue(it.hasNext());
		keys.add(it.next().getKey());
		assertTrue(it.hasNext());
		keys.add(it.next().getKey());
		assertTrue(it.hasNext()==false);
		assertTrue(keys.contains("2") && keys.contains("3"));
		
		//get 1 3
		it=table.select("name=1 and dept=2 or name=a and dept=x or dept=A and team=B").iterator();
		keys.clear();
		assertTrue(it.hasNext());
		keys.add(it.next().getKey());
		assertTrue(it.hasNext());
		keys.add(it.next().getKey());
		assertTrue(it.hasNext()==false);
		assertTrue(keys.contains("1") && keys.contains("3"));
		
		table.deleteRow("1");
		table.deleteRow("2");
		table.deleteRow("3");

		table.close();
	}

	@Test(expected = Exception.class)
	public void testInsertLackIndex() throws Exception {
		UserTable table = UserTableFactory.getTable(tableName);
		table.open();

		table.insert("1", "dept=2 team=3 a4=4 a5=5 a6=6");

		table.close();
	}

	@Test(expected = Exception.class)
	public void testSelectWrongIndex() throws Exception {
		UserTable table = UserTableFactory.getTable(tableName);
		table.open();
		try {
			table.select("dept=2 and team=3 and a4=4");
		} catch (Exception e) {
			table.close();
			throw e;
		}
	}
}
