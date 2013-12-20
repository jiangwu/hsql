package com.hsql.core.test;

import static org.junit.Assert.*;

import java.util.Iterator;

import org.junit.Test;

import com.hsql.core.AdminUtil;
import com.hsql.core.UserRow;
import com.hsql.core.UserTable;
import com.hsql.core.UserTableFactory;
import com.hsql.core.Utils;
import com.sun.xml.bind.Util;

public class CFTest {
	String tableName="jwTest";
	
	@Test
	public void rebuild() throws Exception{
		AdminUtil admin=new AdminUtil();
		admin.deleteTable(tableName);
		admin.createTable(tableName, new String[]{"MetaInfo:id", "MetaInfo:region"});
		assertTrue(admin.isTableValid(tableName));
		
		UserTable table = UserTableFactory.getTable(tableName);
		table.open();
		table.insert("1", "id=1 region=nam");
		table.insert("2", "id=2 region=eu");
		
		table.reBuildIndex();
		table.close();
	}
	
	@Test
	public void createIndex() throws Exception{
		AdminUtil admin=new AdminUtil();
		admin.createIndex(tableName, new String[]{"MetaInfo:id", "MetaInfo:region", "MetaInfo:FIRMACCT"});
		
	}
	
	@Test 
	public void buildIndex4existingTable() throws Exception {
		AdminUtil admin=new AdminUtil();
		admin.createIndex("Credit.Position.RT", new String[]{"MetaInfo:CUSIP", "MetaInfo:DESK"});
		
		
		UserTable table = UserTableFactory.getTable("Credit.Position.RT");
		table.open();
		table.reBuildIndex();
		
	
		table.close();
	}
	
	
	@Test
	public void search() throws Exception{
		UserTable table = UserTableFactory.getTable("Credit.Position.RT");
		table.open();
		
		Iterable<UserRow> res = table.select("CUSIP=001084AK8");
		Iterator<UserRow> it = res.iterator();
		while(it.hasNext())
			Utils.printRow(it.next());		
		table.close();
	}
	

}
