package om.chance.bigdata.format;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;

public class Test {
	public static void main(String[] args) throws IOException {
//		BufferedReader br = new BufferedReader(new FileReader("src/main/resources/fx.doc"));
////		new bufferre
////		new filer
//		String line ="";
//		while ((line = br.readLine()) != null) {   
//		      System.out.println(line.getBytes()); 
//		}  
//		br.close(); 
		
		
		File file=new File("src/main/resources/fx.doc");
		BufferedReader br=new BufferedReader(new InputStreamReader(new FileInputStream(file),"GBK"));
		String s = "范祥";
		System.out.println(Arrays.toString(s.getBytes("utf-8")));
		System.out.println(Arrays.toString(s.getBytes("GBK")));
		String line=null;
		        while((line=br.readLine())!=null){
		        	System.out.println(Arrays.toString(line.getBytes()));
//		           System.out.println(new String(line.getBytes("utf-8")));
		        }
	}
}
