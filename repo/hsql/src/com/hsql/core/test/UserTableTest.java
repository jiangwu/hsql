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
	static String colFamily="f1";

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
		admin.open();
		
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
		
		admin.close();

	}
	

	@Test
	public void testValueLength() throws Exception {
		UserTable table = UserTableFactory.getTable(tableName);
		table.open();

		table.insert("1", "f1:a1=1 f1:a2=2 f1:a3=3 f1:a4=4 f1:a5=5 f1:a6=6");
		table.insert("2", "f1:a1=123 f1:a2=2 f1:a3=3 f1:a4=4 f1:a5=5 f1:a6=6");

		Iterator<UserRow> it = table.select("f1:a1=1").iterator();
		assertTrue(it.hasNext());
		assertTrue(it.next().getKey().equals("1"));
		assertTrue(it.hasNext()==false);
		
		it = table.select("f1:a1=123").iterator();
		assertTrue(it.hasNext());
		assertTrue(it.next().getKey().equals("2"));
		assertTrue(it.hasNext()==false);
		
		it = table.select("f1:a1=12").iterator();
		assertTrue(it.hasNext()==false);
		
		
		table.deleteRow("1");
		table.deleteRow("2");

		table.close();
	}
	
	@Test
	public void testInsert1Select1() throws Exception {
		UserTable table = UserTableFactory.getTable(tableName);
		table.open();

		table.insert("1", "f1:a1=1 f1:a2=2 f1:a3=3 f1:a4=4 f1:a5=5 f1:a6=6");

		Iterator<UserRow> it = table.select("f1:a1=1").iterator();
		assertTrue(it.hasNext());
		assertTrue(it.next().getKey().equals("1"));

		it = table.select("f1:a2=2").iterator();
		assertTrue(it.hasNext());
		assertTrue(it.next().getKey().equals("1"));
		
		it = table.select("f1:a3=3").iterator();
		assertTrue(it.hasNext());
		assertTrue(it.next().getKey().equals("1"));

		it = table.select("f1:a1=1 and f1:a3=3").iterator();
		assertTrue(it.hasNext());
		assertTrue(it.next().getKey().equals("1"));

		it = table.select("f1:a2=2 and f1:a3=3").iterator();
		assertTrue(it.hasNext());
		assertTrue(it.next().getKey().equals("1"));

		table.deleteRow("1");

		table.close();
	}

	@Test
	public void testInsert2Select1() throws Exception {
		UserTable table = UserTableFactory.getTable(tableName);
		table.open();

		table.insert("1", "f1:a1=1 f1:a2=2 f1:a3=3 f1:a4=4 f1:a5=5 f1:a6=6");


		table.insert("2", "f1:a1=a f1:a2=b f1:a3=c f1:a4=d f1:a5=e f1:a6=f");
		
		Iterator<UserRow> it = table.select("f1:a1=a").iterator();
		assertTrue(it.hasNext());
		assertTrue(it.next().getKey().equals("2"));

		it = table.select("f1:a2=b").iterator();
		assertTrue(it.hasNext());
		assertTrue(it.next().getKey().equals("2"));

		it = table.select("f1:a3=c").iterator();
		assertTrue(it.hasNext());
		assertTrue(it.next().getKey().equals("2"));
		
		it = table.select("f1:a1=a and f1:a3=c").iterator();
		assertTrue(it.hasNext());
		assertTrue(it.next().getKey().equals("2"));

		it = table.select("f1:a2=b and f1:a3=c").iterator();
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

		table.insert("1", "f1:a1=1 f1:a2=2 f1:a3=3 f1:a4=4 f1:a5=5 f1:a6=6");


		table.insert("2", "f1:a1=a f1:a2=2 f1:a3=3 f1:a4=d f1:a5=e f1:a6=f");



		Iterator<UserRow> it = table.select("f1:a2=2 and f1:a3=3").iterator();
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

		table.insert("1", "f1:a1=1 f1:a2=2 f1:a3=3 f1:a4=4 f1:a5=5 f1:a6=6");


		table.insert("2", "f1:a1=a f1:a2=b f1:a3=3 f1:a4=d f1:a5=e f1:a6=f");
		

		table.insert("3", "f1:a1=a f1:a2=A f1:a3=3 f1:a4=d f1:a5=e f1:a6=f");
		

		Iterator<UserRow> it = table.select("f1:a2=2 or f1:a2=b").iterator();
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

		table.insert("1", "f1:a1=1 f1:a2=2 f1:a3=3 f1:a4=4 f1:a5=5 f1:a6=6");

		Map<String, String> indexes = new HashMap<String, String>();
		indexes.put("f1:a1", "1");
		
		Iterator<UserRow> it = table.select("f1:a1=1").iterator();
		
		assertTrue(it.hasNext());

		assertTrue(it.next().getKey().equals("1"));

		table.deleteRow("1");
		
		it = table.select("f1:a1=1").iterator();
		
		assertTrue(it.hasNext() == false);
		

		table.close();
	}
	
	@Test
	public void testCommandSingle() throws Exception {
		UserTable table = UserTableFactory.getTable(tableName);
		table.open();

		table.insert("1", "f1:a1=1 f1:a2=2 f1:a3=3 f1:a4=4 f1:a5=5 f1:a6=6");

		Iterator<UserRow> it = table.select("f1:a2=2").iterator();
		assertTrue(it.hasNext()==true);
		assertTrue(it.next().getKey().equals("1"));

		table.deleteRow("1");


		table.close();
	}
	
	@Test
	public void testCommands() throws Exception {
		UserTable table = UserTableFactory.getTable(tableName);
		table.open();

		table.insert("1", "f1:a1=1 f1:a2=2 f1:a3=3 f1:a4=4 f1:a5=5 f1:a6=6");


		table.insert("2", "f1:a1=a f1:a2=b f1:a3=3 f1:a4=d f1:a5=e f1:a6=f");
		

		table.insert("3", "f1:a1=a f1:a2=A f1:a3=B f1:a4=d f1:a5=e f1:a6=f");
		
		Iterator<UserRow> it = table.select("f1:a1=a and f1:a2=b").iterator();
		assertTrue(it.hasNext());
		assertTrue(it.next().getKey().equals("2"));
		assertTrue(it.hasNext()==false);
		
		it = table.select("f1:a1=a or f1:a3=B").iterator();
		Set<String> keys=new HashSet<String>();
		assertTrue(it.hasNext());
		keys.add(it.next().getKey());
		assertTrue(it.hasNext());
		keys.add(it.next().getKey());
		assertTrue(it.hasNext()==false);
		assertTrue(keys.contains("2") && keys.contains("3"));
		
		//get 1 3
		it=table.select("f1:a1=1 and f1:a2=2 or f1:a1=a and f1:a2=x or f1:a2=A and f1:a3=B").iterator();
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

		table.insert("1", "f1:a2=2 f1:a3=3 f1:a4=4 f1:a5=5 f1:a6=6");

		table.close();
	}

	@Test(expected = Exception.class)
	public void testSelectWrongIndex() throws Exception {
		UserTable table = UserTableFactory.getTable(tableName);
		table.open();
		try {
			table.select("f1:a2=2 and f1:a3=3 and f1:a4=4");
		} catch (Exception e) {
			table.close();
			throw e;
		}
	}
}
