/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.github.dangxia.study.storm.state;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.Stream;
import storm.trident.TridentTopology;
import storm.trident.operation.CombinerAggregator;
import storm.trident.operation.Filter;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.spout.IBatchSpout;
import storm.trident.tuple.TridentTuple;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

/**
 * @author XuehuiHe
 * @date 2014年6月27日
 */
public class GroupByTopology {
	private static Logger logger = LoggerFactory
			.getLogger(GroupByTopology.class);

	public static void main(String[] args) {
		Config config = new Config();
		config.setMaxSpoutPending(1);

		TridentTopology topology = new TridentTopology();
		Stream stream = topology
				.newStream("jsldjfl", new IntSpout())
				.parallelismHint(2)
				.shuffle()
				.groupBy(new Fields("k"))
				.aggregate(new Fields("k", "v"),
						new CombinerAggregator<Integer>() {

							@Override
							public Integer init(TridentTuple tuple) {
								logger.info("key:" + tuple.getString(0)
										+ "\tthread_id:"
										+ Thread.currentThread().getId());
								return tuple.getInteger(1);
							}

							@Override
							public Integer combine(Integer val1, Integer val2) {
								return val1 > val2 ? val1 : val2;
							}

							@Override
							public Integer zero() {
								return 0;
							}
						}, new Fields("v2")).parallelismHint(2)
				.each(new Fields("k", "v2"), new Filter() {

					@Override
					public void prepare(Map conf,
							TridentOperationContext context) {
					}

					@Override
					public void cleanup() {

					}

					@Override
					public boolean isKeep(TridentTuple tuple) {
						logger.info("thread_id:"
								+ Thread.currentThread().getId() + "\t"
								+ tuple.toString());
						return false;
					}
				});

		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("sdjflsjd", config, topology.build());
	}

	public static class IntSpout implements IBatchSpout {
		private Map<String, Long> map;
		private Random r;

		@Override
		public void open(@SuppressWarnings("rawtypes") Map conf,
				TopologyContext context) {
			map = new HashMap<String, Long>();
			map.put("one", 0l);
			map.put("two", 0l);
			map.put("three", 0l);
			r = new Random();
		}

		@Override
		public void emitBatch(long batchId, TridentCollector collector) {
			for (Entry<String, Long> entry : map.entrySet()) {
				String key = entry.getKey();
				long item = 0l;
				int times = r.nextInt(3);
				for (int i = 0; i < times; i++) {
					int next = r.nextInt(10);
					collector.emit(new Values(key, next));
					if (item < next) {
						item = next;
					}
				}
				logger.info("thread_id:" + Thread.currentThread().getId()
						+ "\tkey:" + key + "\tcurr:" + item);
			}

		}

		@Override
		public void ack(long batchId) {
		}

		@Override
		public void close() {
		}

		@SuppressWarnings("rawtypes")
		@Override
		public Map getComponentConfiguration() {
			return null;
		}

		@Override
		public Fields getOutputFields() {
			return new Fields("k", "v");
		}
	}
}
