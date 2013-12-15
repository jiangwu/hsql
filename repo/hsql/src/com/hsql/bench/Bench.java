package com.hsql.bench;

import java.util.ArrayList;
import java.util.List;

public class Bench {

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		List<List<Long>> res=new ArrayList<List<Long>>();
		for(int colSize=4;colSize<=16;colSize+=4){
			TestSetting.colSize=colSize;
			CreateTestTable.run();
			long queryTime=QueryTestTable.run();
			long scanTime=ScanTestTable.run();

			List<Long> row=new ArrayList<Long>();
			row.add((long) colSize*colSize*colSize*colSize*colSize);
			row.add(scanTime);
			row.add(queryTime);
			res.add(row);
		}
		
		System.out.println("rows, scan, query");
		for(List<Long> row:res){
			System.out.println(row.get(0)+","+row.get(1)+","+row.get(2));
		}

	}

}
