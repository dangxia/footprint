package com.github.dangxia.study.storm;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.operation.BaseFilter;
import storm.trident.tuple.TridentTuple;

public class StormUtil {
	private static Logger log = LoggerFactory.getLogger(StormUtil.class);

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

	public static class PrintFilter extends BaseFilter {
		@Override
		public boolean isKeep(TridentTuple tuple) {
			log.info(tuple.toString());
			return true;
		}

	}

	public static class DelayFilter extends BaseFilter {
		private final TimeUnit unit;
		private final long duration;

		public DelayFilter(TimeUnit unit, long duration) {
			this.unit = unit;
			this.duration = duration;
		}

		@Override
		public boolean isKeep(TridentTuple tuple) {
			try {
				unit.sleep(duration);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			return true;
		}

	}
}
