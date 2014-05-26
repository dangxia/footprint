package com.github.dangxia.study.storm.state;

import java.util.Map;
import java.util.Map.Entry;

import org.springframework.context.support.ClassPathXmlApplicationContext;

public class TestSpring {
	public static void main(String[] args) {
		ClassPathXmlApplicationContext cxt = new ClassPathXmlApplicationContext(
				"spring-state.xml");
		OemDao dao = cxt.getBean(OemDao.class);
		Map<String, String> map = dao.getOemidToSpid();
		for (Entry<String, String> entry : map.entrySet()) {
			System.out.println(entry.getKey() + "\t" + entry.getValue());
		}
		cxt.close();
	}
}
