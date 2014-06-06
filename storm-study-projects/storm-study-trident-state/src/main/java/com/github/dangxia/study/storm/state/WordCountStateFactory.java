/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.github.dangxia.study.storm.state;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.state.State;
import storm.trident.state.StateFactory;
import storm.trident.state.ValueUpdater;
import storm.trident.testing.MemoryMapState;
import backtype.storm.task.IMetricsContext;

/**
 * @author XuehuiHe
 * @date 2014年6月5日
 */
public class WordCountStateFactory implements StateFactory {
	String _id;
	private static final Logger log = LoggerFactory
			.getLogger(WordCountStateFactory.class);
	private TimeUnit unit;
	private long duration;

	public WordCountStateFactory(TimeUnit unit, long duration) {
		_id = UUID.randomUUID().toString();
		this.unit = unit;
		this.duration = duration;
	}

	@Override
	public State makeState(@SuppressWarnings("rawtypes") Map conf,
			IMetricsContext metrics, int partitionIndex, int numPartitions) {
		return new TestMemoryMapState(_id + partitionIndex);
	}

	public class TestMemoryMapState extends MemoryMapState<Long> {
		private final AtomicLong running;

		public TestMemoryMapState(String id) {
			super(id);
			running = new AtomicLong(0l);
		}

		protected void before() {
			long r = running.incrementAndGet();
			log.info("running num:" + r);
			try {
				unit.sleep(duration);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		protected void after() {
			running.decrementAndGet();
		}

		@Override
		public List<Long> multiUpdate(List<List<Object>> keys,
				@SuppressWarnings("rawtypes") List<ValueUpdater> updaters) {
			log.info("running multiUpdate");
			before();
			List<Long> list = super.multiUpdate(keys, updaters);
			after();
			return list;
		}

		@Override
		public void multiPut(List<List<Object>> keys, List<Long> vals) {
			log.info("running multiPut");
			before();
			super.multiPut(keys, vals);
			after();
		}

		@Override
		public List<Long> multiGet(List<List<Object>> keys) {
			log.info("running multiGet");
			before();
			List<Long> list = super.multiGet(keys);
			after();
			return list;
		}

	}
}
