package com.chance.dom4jtest;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.util.Iterator;
import java.util.List;

import org.dom4j.Attribute;
import org.dom4j.Document;
import org.dom4j.Element;
import org.dom4j.io.OutputFormat;
import org.dom4j.io.SAXReader;
import org.dom4j.io.XMLWriter;
import org.junit.Test;

public class Domtest {

	@Test
	public void test() throws Exception {
		SAXReader reader = new SAXReader();
		Document document = reader.read(new File("sida.xml"));
		Element node = document.getRootElement();
		listNode(node);
		Element element = node.element("红楼梦");
		Attribute attr = element.attribute("id");
		element.remove(attr);
		element.addAttribute("name", "作者");
		
		Element newElement = element.addElement("朝代");
		newElement.setText("清朝");
		Element author = element.element("作者");
		boolean flage = element.remove(author);
		System.out.println(flage);
		element.addCDATA("红楼梦，是一部爱情小说。");
		writer(document);
		
	}
	
	public void writer(Document document) throws Exception {
		OutputFormat format = OutputFormat.createPrettyPrint();
		format.setEncoding("utf-8");

		XMLWriter writer = new XMLWriter(new OutputStreamWriter(new FileOutputStream(new File("a.xml")),"utf-8"),format);
		writer.write(document);
		writer.flush();
		writer.close();
	}
	
	public void listNode(Element node) {
		System.out.println("当前节点的名称："+node.getName());
		List<Attribute> list = node.attributes();
		 for(Attribute attr : list) {
			 System.out.println(attr.getText() +"-------"+attr.getName()+"------"+attr.getValue());
		 }
		 
		 if(!(node.getTextTrim().equals(""))) {
			 System.out.println("文本内容：：："+node.getText());
		 }
		 
		 Iterator<Element> it = node.elementIterator();
		 while(it.hasNext()) {
			 Element e = it.next();
			 listNode(e);
		 }
	}
	
	public void elementMethod(Element node) {
		Element e = node.element("西游记");
		Element author = e.element("作者");
		System.out.println(e.getName()+"-------"+author.getText());
		List<Element> authors = e.elements("作者");
		for(Element aut : authors) {
			System.out.println(aut.getText());
		}
		
		List<Element> elemens = e.elements();
		
		for(Element el:elemens) {
			System.out.println(el.getText());
		}
	}
}
