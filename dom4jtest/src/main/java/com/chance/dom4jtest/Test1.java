package com.chance.dom4jtest;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

public class Test1 {
	public static void main(String[] args) throws IOException {
//	new BufferedReader(new InputStreamReader(System.in));
		BufferedReader reader = new BufferedReader(new FileReader("C:\\Users\\ww\\Desktop\\data\\badaqu\\badaqushuju"));
		String line=null;
		int i=0;
		int j=0;
		Map<String , ArrayList<String>> map = new HashMap<String, ArrayList<String>>();
		while ((line=reader.readLine())!=null) {
			String[] strings = line.split("\t");
			if(strings.length==10) {
//				i = i+1;
				String aa = strings[0].trim()+"\t"+strings[1].trim()+"\t"+strings[2].trim()+"\t"+strings[3].trim()+"\t"+strings[4].trim()+"\t"+strings[5].trim()+"\t"
						+strings[6].trim()+"\t"+strings[7].trim()+"\t"+strings[8].trim()+"\t"+strings[9].trim();
				if(!map.containsKey(aa)) {
					map.put(aa, new ArrayList());
				}else {
					map.get(aa).add(aa);
				}
			}
			j++;
		}
		System.out.println("j= "+j);
		System.out.println("map size is :"+map.size());
		Map<String, Integer> uniqsize = new HashMap<>();
		for(Entry<String, ArrayList<String>> entrySet : map.entrySet()) {
//			entrySet.getKey()
			uniqsize.put(entrySet.getKey(), entrySet.getValue().size());
		}
		BufferedWriter writer = new BufferedWriter(new FileWriter("result"));
		for(Entry<String, Integer> len : uniqsize.entrySet()) {
			if(len.getValue()>1) {
				String aa = len.getKey()+"\t"+len.getValue();
				writer.write(aa+"\n");
			}
		}
		System.out.println("successed!!!!!!!!!!!!!");
		reader.close();
		writer.close();
	}

}
