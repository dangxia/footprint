package com.github.dangxia.study.storm;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class StormUtil {
	public static List<String> getRandomStrs(int size) {
		List<String> strs = new ArrayList<String>();
		Random r = new Random();
		for (int i = 0; i < size; i++) {
			strs.add(r.nextDouble() + "");
		}
		return strs;
	}

	public static List<String> getRandomStrs() {
		return getRandomStrs(20);
	}
}
