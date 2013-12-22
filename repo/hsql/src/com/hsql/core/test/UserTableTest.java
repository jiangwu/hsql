package com.hsql.core.test;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.hsql.core.IndexAdminImpl;
import com.hsql.core.UserRow;
import com.hsql.core.UserTable;
import com.hsql.core.UserTableFactory;

public class UserTableTest {
	static IndexAdminImpl admin = null;
	static HBaseAdmin hadmin=null;
	static String tableName = "unitTestTable";
	static String colFamily="Meta";

	@BeforeClass
	static public void init() throws Exception {
		HBaseAdmin hadmin=new HBaseAdmin(new Configuration());
		if(!hadmin.isTableAvailable(tableName)){
			HTableDescriptor tdesc=new HTableDescriptor(tableName);
			HColumnDescriptor cdesc=new HColumnDescriptor(colFamily);
			tdesc.addFamily(cdesc);
			hadmin.createTable(tdesc);
		}else{
			HTableDescriptor tdesc=hadmin.getTableDescriptor(tableName.getBytes());
			hadmin.disableTable(tableName);
			hadmin.deleteTable(tableName);
			hadmin.createTable(tdesc);
		}
		hadmin.close();
		admin = new IndexAdminImpl();
		
		
		if (admin.isTableIndexed(tableName)) {
			admin.deleteIndex(tableName);
		}
		admin.buildIndex(tableName, new String[] { colFamily+":a1", colFamily+":a2", colFamily+":a3" });
	}

	@AfterClass
	static public void cleanup() throws IOException {
		admin.deleteIndex(tableName);
		
		hadmin=new HBaseAdmin(new Configuration());
		hadmin.disableTable(tableName);
		hadmin.deleteTable(tableName);
		hadmin.close();

	}
	

	@Test
	public void testValueLength() throws Exception {
		UserTable table = UserTableFactory.getTable(tableName);
		table.open();

		table.insert("1", "a1=1 a2=2 a3=3 a4=4 a5=5 a6=6");
		table.insert("2", "a1=123 a2=2 a3=3 a4=4 a5=5 a6=6");

		Iterator<UserRow> it = table.select("a1=1").iterator();
		assertTrue(it.hasNext());
		assertTrue(it.next().getKey().equals("1"));
		assertTrue(it.hasNext()==false);
		
		it = table.select("a1=123").iterator();
		assertTrue(it.hasNext());
		assertTrue(it.next().getKey().equals("2"));
		assertTrue(it.hasNext()==false);
		
		it = table.select("a1=12").iterator();
		assertTrue(it.hasNext()==false);
		
		
		table.deleteRow("1");
		table.deleteRow("2");

		table.close();
	}
	
	@Test
	public void testInsert1Select1() throws Exception {
		UserTable table = UserTableFactory.getTable(tableName);
		table.open();

		table.insert("1", "a1=1 a2=2 a3=3 a4=4 a5=5 a6=6");

		Iterator<UserRow> it = table.select("a1=1").iterator();
		assertTrue(it.hasNext());
		assertTrue(it.next().getKey().equals("1"));

		it = table.select("a2=2").iterator();
		assertTrue(it.hasNext());
		assertTrue(it.next().getKey().equals("1"));
		
		it = table.select("a3=3").iterator();
		assertTrue(it.hasNext());
		assertTrue(it.next().getKey().equals("1"));

		it = table.select("a1=1 and a3=3").iterator();
		assertTrue(it.hasNext());
		assertTrue(it.next().getKey().equals("1"));

		it = table.select("a2=2 and a3=3").iterator();
		assertTrue(it.hasNext());
		assertTrue(it.next().getKey().equals("1"));

		table.deleteRow("1");

		table.close();
	}

	@Test
	public void testInsert2Select1() throws Exception {
		UserTable table = UserTableFactory.getTable(tableName);
		table.open();

		table.insert("1", "a1=1 a2=2 a3=3 a4=4 a5=5 a6=6");


		table.insert("2", "a1=a a2=b a3=c a4=d a5=e a6=f");
		
		Iterator<UserRow> it = table.select("a1=a").iterator();
		assertTrue(it.hasNext());
		assertTrue(it.next().getKey().equals("2"));

		it = table.select("a2=b").iterator();
		assertTrue(it.hasNext());
		assertTrue(it.next().getKey().equals("2"));

		it = table.select("a3=c").iterator();
		assertTrue(it.hasNext());
		assertTrue(it.next().getKey().equals("2"));
		
		it = table.select("a1=a and a3=c").iterator();
		assertTrue(it.hasNext());
		assertTrue(it.next().getKey().equals("2"));

		it = table.select("a2=b and a3=c").iterator();
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

		table.insert("1", "a1=1 a2=2 a3=3 a4=4 a5=5 a6=6");


		table.insert("2", "a1=a a2=2 a3=3 a4=d a5=e a6=f");



		Iterator<UserRow> it = table.select("a2=2 and a3=3").iterator();
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

		table.insert("1", "a1=1 a2=2 a3=3 a4=4 a5=5 a6=6");


		table.insert("2", "a1=a a2=b a3=3 a4=d a5=e a6=f");
		

		table.insert("3", "a1=a a2=A a3=3 a4=d a5=e a6=f");
		

		Iterator<UserRow> it = table.select("a2=2 or a2=b").iterator();
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

		table.insert("1", "a1=1 a2=2 a3=3 a4=4 a5=5 a6=6");

		Map<String, String> indexes = new HashMap<String, String>();
		indexes.put("a1", "1");
		
		Iterator<UserRow> it = table.select("a1=1").iterator();
		
		assertTrue(it.hasNext());

		assertTrue(it.next().getKey().equals("1"));

		table.deleteRow("1");
		
		it = table.select("a1=1").iterator();
		
		assertTrue(it.hasNext() == false);
		

		table.close();
	}
	
	@Test
	public void testCommandSingle() throws Exception {
		UserTable table = UserTableFactory.getTable(tableName);
		table.open();

		table.insert("1", "a1=1 a2=2 a3=3 a4=4 a5=5 a6=6");

		Iterator<UserRow> it = table.select("a2=2").iterator();
		assertTrue(it.hasNext()==true);
		assertTrue(it.next().getKey().equals("1"));

		table.deleteRow("1");


		table.close();
	}
	
	@Test
	public void testCommands() throws Exception {
		UserTable table = UserTableFactory.getTable(tableName);
		table.open();

		table.insert("1", "a1=1 a2=2 a3=3 a4=4 a5=5 a6=6");


		table.insert("2", "a1=a a2=b a3=3 a4=d a5=e a6=f");
		

		table.insert("3", "a1=a a2=A a3=B a4=d a5=e a6=f");
		
		Iterator<UserRow> it = table.select("a1=a and a2=b").iterator();
		assertTrue(it.hasNext());
		assertTrue(it.next().getKey().equals("2"));
		assertTrue(it.hasNext()==false);
		
		it = table.select("a1=a or a3=B").iterator();
		Set<String> keys=new HashSet<String>();
		assertTrue(it.hasNext());
		keys.add(it.next().getKey());
		assertTrue(it.hasNext());
		keys.add(it.next().getKey());
		assertTrue(it.hasNext()==false);
		assertTrue(keys.contains("2") && keys.contains("3"));
		
		//get 1 3
		it=table.select("a1=1 and a2=2 or a1=a and a2=x or a2=A and a3=B").iterator();
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

		table.insert("1", "a2=2 a3=3 a4=4 a5=5 a6=6");

		table.close();
	}

	@Test(expected = Exception.class)
	public void testSelectWrongIndex() throws Exception {
		UserTable table = UserTableFactory.getTable(tableName);
		table.open();
		try {
			table.select("a2=2 and a3=3 and a4=4");
		} catch (Exception e) {
			table.close();
			throw e;
		}
	}
}
