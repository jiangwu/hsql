package com.hsql.core;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

class IndexCreator {
	/**
	 * 
	 * @param big
	 * @param small
	 * @return components in set big but not in set small
	 */
	private static TreeSet<Integer> difference(Set<Integer> big,
			Set<Integer> small) {
		TreeSet<Integer> res = new TreeSet<Integer>();
		for (Integer i : big) {
			if (!small.contains(i)) {
				res.add(i);
			}
		}
		return res;
	}

	/**
	 * 
	 * @param set
	 * @param i
	 * @return
	 */
	private static TreeSet<Integer> copyMinus(TreeSet<Integer> set, int i) {
		TreeSet<Integer> res = new TreeSet<Integer>();
		for (Integer j : set) {
			if (i != j)
				res.add(j);
		}
		return res;
	}

	/**
	 * 
	 * @param set
	 * @param i
	 * @return a set with all components from input set plus input i
	 */
	private static TreeSet<Integer> copyAdd(Set<Integer> set, int i) {
		TreeSet<Integer> res = new TreeSet<Integer>();
		for (Integer j : set) {
			res.add(j);
		}
		res.add(i);
		return res;
	}

	/**
	 * 
	 * @param set
	 * @return all non-empty subsets of input set
	 */
	private static Set<TreeSet<Integer>> subSets(TreeSet<Integer> set) {
		Set<TreeSet<Integer>> res = new HashSet<TreeSet<Integer>>();
		if (set.size() <= 1) {
			res.add(set);
			return res;
		}

		for (Integer i : set) {
			TreeSet<Integer> sub = copyMinus(set, i);
			Set<TreeSet<Integer>> resi = subSets(sub);
			for (TreeSet<Integer> s : resi) {
				res.add(s);
				res.add(copyAdd(s, i));
				TreeSet<Integer> single = new TreeSet<Integer>();
				single.add(i);
				res.add(single);

			}
		}
		return res;

	}
	
	static String getSearchKey(Map<String, String> indexes, TreeSet<String> indexNames) throws Exception{
		{
			TreeMap<String, String> sortedIndexes = new TreeMap<String, String>();
			sortedIndexes.putAll(indexes);
			
			Map<String, Integer> nameSeq=new HashMap<String, Integer>();
			int count=0;
			for(String name: indexNames){
				nameSeq.put(name, count);
				count++;
			}

			StringBuffer key = new StringBuffer();
			for (String col : sortedIndexes.keySet()) {
				key.append("~");
				key.append(nameSeq.get(col));
				key.append(sortedIndexes.get(col));
			}

			int groupId;
			boolean containLast = false;
			// if one col is the last col in all indexed cols
			if (indexNames.descendingSet().first()
					.equals(sortedIndexes.descendingKeySet().first())) {
				containLast = true;
			}

			if (containLast)
				groupId = indexNames.size() - indexes.size();
			else
				groupId = indexNames.size() - indexes.size() - 1;

			String searchKey = groupId + key.toString();
			return searchKey;
		}
	}

	/**
	 * thread safe
	 * @param indexCol contains column => value pairs
	 * @return a list of keys used to find primary key
	 * @throws Exception 
	 */
	public static List<String> getIndexKeys(Map<String, String> indexCol, String pKey, TreeSet<String> indexNames) throws Exception {
		if(indexCol.size()>indexNames.size() || !indexNames.containsAll(indexCol.keySet())){
			throw new Exception(indexCol+" contains invalid or insufficent indexes");
		}
		TreeMap<String, String> tree = new TreeMap<String, String>();
		tree.putAll(indexCol);
		String[] colName = new String[tree.size()];

		String[] colValue = new String[tree.size()];
		{
			int i = 0;
			for (String key : tree.keySet()) {
				colName[i] = key;
				colValue[i] = tree.get(key);
				i++;
			}
		}

		List<String> keys = new ArrayList<String>();

		int keyLen = colValue.length;

		TreeSet<Integer> wholeSet = new TreeSet<Integer>();
		for (int i = 0; i < keyLen; i++)
			wholeSet.add(i);

		Set<TreeSet<Integer>> subs = subSets(copyMinus(wholeSet, keyLen - 1));
		TreeSet<Integer> last = new TreeSet<Integer>();
		subs.add(last); // last is empty

		for (TreeSet<Integer> s : subs) {
			s.add(keyLen - 1);
			int groupId = keyLen - s.size();

			StringBuffer key = new StringBuffer();
			key.append(groupId);
			for (Integer i : s) {
				key.append("~");
				key.append(i);
				key.append(colValue[i]);
			}

			TreeSet<Integer> rem = difference(wholeSet, s);

			//TODO can be delete ?
			for (Integer i : rem) {
				key.append("~");
				key.append(i);
				key.append(colValue[i]);
			}
			key.append("~");
			key.append(pKey);
			keys.add(key.toString());
		}
		Collections.sort(keys);
		return keys;

	}

}
