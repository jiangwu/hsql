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

public class UseCaseTest {
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
		admin.open();
		
		if (admin.isTableIndexed(tableName)) {
			admin.deleteIndex(tableName);
		}
		admin.buildIndex(tableName, new String[] { colFamily+":name", colFamily+":dept", colFamily+":team" });
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

		table.insert("1", "Meta:name=abc.def Meta:dept=R&D Meta:team=hbase.hsql Meta:title=avp Meta:role=developer Meta:level=senir");
		table.insert("2", "Meta:name=a.b Meta:dept=R&D Meta:team=hbase.hsql Meta:title=avp Meta:role=developer Meta:level=senir");

		Iterator<UserRow> it = table.select("Meta:name=abc.def").iterator();
		assertTrue(it.hasNext());
		assertTrue(it.next().getKey().equals("1"));
		assertTrue(it.hasNext()==false);
		
		it = table.select("Meta:name=a.b").iterator();
		assertTrue(it.hasNext());
		assertTrue(it.next().getKey().equals("2"));
		assertTrue(it.hasNext()==false);
		
		it = table.select("Meta:name=ad").iterator();
		assertTrue(it.hasNext()==false);
		
		it=table.select("Meta:dept=R&D").iterator();
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

		table.insert("1", "Meta:name=1 Meta:dept=2 Meta:team=hbase.hsql Meta:title=avp Meta:role=developer Meta:level=senir");

		Iterator<UserRow> it = table.select("Meta:name=1").iterator();
		assertTrue(it.hasNext());
		assertTrue(it.next().getKey().equals("1"));

		it = table.select("Meta:dept=2").iterator();
		assertTrue(it.hasNext());
		assertTrue(it.next().getKey().equals("1"));
		
		it = table.select("Meta:team=hbase.hsql").iterator();
		assertTrue(it.hasNext());
		assertTrue(it.next().getKey().equals("1"));

		it = table.select("Meta:name=1 and Meta:team=hbase.hsql").iterator();
		assertTrue(it.hasNext());
		assertTrue(it.next().getKey().equals("1"));

		it = table.select("Meta:dept=2 and Meta:team=hbase.hsql").iterator();
		assertTrue(it.hasNext());
		assertTrue(it.next().getKey().equals("1"));

		table.deleteRow("1");

		table.close();
	}

	@Test
	public void testInsert2Select1() throws Exception {
		UserTable table = UserTableFactory.getTable(tableName);
		table.open();

		table.insert("1", "Meta:name=1 Meta:dept=2 Meta:team=hbase.hsql Meta:title=avp Meta:role=developer Meta:level=senir");


		table.insert("2", "Meta:name=a Meta:dept=b Meta:team=c Meta:a4=d Meta:a5=e Meta:a6=f");
		
		Iterator<UserRow> it = table.select("Meta:name=a").iterator();
		assertTrue(it.hasNext());
		assertTrue(it.next().getKey().equals("2"));

		it = table.select("Meta:dept=b").iterator();
		assertTrue(it.hasNext());
		assertTrue(it.next().getKey().equals("2"));

		it = table.select("Meta:team=c").iterator();
		assertTrue(it.hasNext());
		assertTrue(it.next().getKey().equals("2"));
		
		it = table.select("Meta:name=a and Meta:team=c").iterator();
		assertTrue(it.hasNext());
		assertTrue(it.next().getKey().equals("2"));

		it = table.select("Meta:dept=b and Meta:team=c").iterator();
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

		table.insert("1", "Meta:name=1 Meta:dept=2 Meta:team=hbase.hsql Meta:title=avp Meta:role=developer Meta:level=senir");


		table.insert("2", "Meta:name=a Meta:dept=2 Meta:team=hbase.hsql Meta:a4=d Meta:a5=e Meta:a6=f");



		Iterator<UserRow> it = table.select("Meta:dept=2 and Meta:team=hbase.hsql").iterator();
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

		table.insert("1", "Meta:name=1 Meta:dept=2 Meta:team=hbase.hsql Meta:title=avp Meta:role=developer Meta:level=senir");


		table.insert("2", "Meta:name=a Meta:dept=b Meta:team=hbase.hsql Meta:a4=d Meta:a5=e Meta:a6=f");
		

		table.insert("3", "Meta:name=a Meta:dept=A Meta:team=hbase.hsql Meta:a4=d Meta:a5=e Meta:a6=f");
		

		Iterator<UserRow> it = table.select("Meta:dept=2 or Meta:dept=b").iterator();
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

		table.insert("1", "Meta:name=1 Meta:dept=2 Meta:team=hbase.hsql Meta:title=avp Meta:role=developer Meta:level=senir");

		Map<String, String> indexes = new HashMap<String, String>();
		indexes.put("name", "1");
		
		Iterator<UserRow> it = table.select("Meta:name=1").iterator();
		
		assertTrue(it.hasNext());

		assertTrue(it.next().getKey().equals("1"));

		table.deleteRow("1");
		
		it = table.select("Meta:name=1").iterator();
		
		assertTrue(it.hasNext() == false);
		

		table.close();
	}
	
	@Test
	public void testCommandSingle() throws Exception {
		UserTable table = UserTableFactory.getTable(tableName);
		table.open();

		table.insert("1", "Meta:name=1 Meta:dept=2 Meta:team=hbase.hsql Meta:title=avp Meta:role=developer Meta:level=senir");

		Iterator<UserRow> it = table.select("Meta:dept=2").iterator();
		assertTrue(it.hasNext()==true);
		assertTrue(it.next().getKey().equals("1"));

		table.deleteRow("1");


		table.close();
	}
	
	@Test
	public void testCommands() throws Exception {
		UserTable table = UserTableFactory.getTable(tableName);
		table.open();

		table.insert("1", "Meta:name=1 Meta:dept=2 Meta:team=hbase.hsql Meta:title=avp Meta:role=developer Meta:level=senir");


		table.insert("2", "Meta:name=a Meta:dept=b Meta:team=hbase.hsql Meta:a4=d Meta:a5=e Meta:a6=f");
		

		table.insert("3", "Meta:name=a Meta:dept=A Meta:team=B Meta:a4=d Meta:a5=e Meta:a6=f");
		
		Iterator<UserRow> it = table.select("Meta:name=a and Meta:dept=b").iterator();
		assertTrue(it.hasNext());
		assertTrue(it.next().getKey().equals("2"));
		assertTrue(it.hasNext()==false);
		
		it = table.select("Meta:name=a or Meta:team=B").iterator();
		Set<String> keys=new HashSet<String>();
		assertTrue(it.hasNext());
		keys.add(it.next().getKey());
		assertTrue(it.hasNext());
		keys.add(it.next().getKey());
		assertTrue(it.hasNext()==false);
		assertTrue(keys.contains("2") && keys.contains("3"));
		
		//get 1 3
		it=table.select("Meta:name=1 and Meta:dept=2 or Meta:name=a and Meta:dept=x or Meta:dept=A and Meta:team=B").iterator();
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

		table.insert("1", "Meta:dept=2 Meta:team=hbase.hsql Meta:title=avp Meta:role=developer Meta:level=senir");

		table.close();
	}

	@Test(expected = Exception.class)
	public void testSelectWrongIndex() throws Exception {
		UserTable table = UserTableFactory.getTable(tableName);
		table.open();
		try {
			table.select("Meta:dept=2 and Meta:team=hbase.hsql and Meta:title=avp");
		} catch (Exception e) {
			table.close();
			throw e;
		}
	}
}
