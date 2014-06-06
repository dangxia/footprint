/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.github.dangxia.study.storm.state;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.builtin.Count;
import storm.trident.spout.IBatchSpout;
import storm.trident.state.BaseQueryFunction;
import storm.trident.testing.MemoryMapState;
import storm.trident.tuple.TridentTuple;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import com.github.dangxia.study.storm.StormUtil.PrintFilter;

/**
 * @author XuehuiHe
 * @date 2014年6月5日
 */
public class WordCountTopology {
	public static Config createConfig() {
		Config config = new Config();
		config.setMaxSpoutPending(2);
		// config.setDebug(true);
		return config;
	}

	public static void main(String[] args) throws InterruptedException {
		Config config = createConfig();
		WordCountSpout spout = new WordCountSpout();
		PrintFilter printer = new PrintFilter();
		// WordCountSpout spout2 = new WordCountSpout();

		TridentTopology topology = new TridentTopology();
		TridentState state = topology
				.newStream("kkkk", spout)
				.each(WordCountSpout.OUT_FIELDS, printer)
				.groupBy(WordCountSpout.OUT_FIELDS)
				.persistentAggregate(
						new WordCountStateFactory(TimeUnit.SECONDS, 1),
						new Count(), new Fields("count"));
		// state.newValuesStream().each(new Fields("word", "count"), printer);

		// topology.newStream("kkkk2", spout).stateQuery(state, function,
		// functionFields)

		LocalCluster cluster = new LocalCluster();
		final LocalDRPC drpc = new LocalDRPC();

		topology.newDRPCStream("query", drpc)
				.each(new Fields("args"), new ArgsFunction(),
						new Fields("word"))
				.stateQuery(state, new Fields("word"), new WordQueryFunction(),
						new Fields("count"))
				.project(new Fields("word", "count"));

		cluster.submitTopology("kkkkkk", config, topology.build());
		/*
		 * new Thread() { public void run() { while (true) {
		 * System.out.println("query one:" + drpc.execute("query", "one"));
		 * System.out.println("query two:" + drpc.execute("query", "two"));
		 * System.out.println("query three:" + drpc.execute("query", "three"));
		 * System.out.println("query all:" + drpc.execute("query",
		 * "one two three four")); try { TimeUnit.MILLISECONDS.sleep(500); }
		 * catch (InterruptedException e) { e.printStackTrace(); } } };
		 * }.start();
		 * 
		 * new Thread() { public void run() { while (true) {
		 * System.out.println("query one:" + drpc.execute("query", "one"));
		 * System.out.println("query two:" + drpc.execute("query", "two"));
		 * System.out.println("query three:" + drpc.execute("query", "three"));
		 * System.out.println("query all:" + drpc.execute("query",
		 * "one two three four")); try { TimeUnit.MILLISECONDS.sleep(500); }
		 * catch (InterruptedException e) { e.printStackTrace(); } } };
		 * }.start();
		 */

	}

	public static final class ArgsFunction extends BaseFunction {

		@Override
		public void execute(TridentTuple tuple, TridentCollector collector) {
			String arg = tuple.getString(0);
			String[] args = arg.trim().split(" ");
			for (String item : args) {
				collector.emit(new Values(item));
			}
		}

	}

	/**
	 * @author XuehuiHe
	 * @date 2014年6月5日
	 */
	public static final class WordQueryFunction extends
			BaseQueryFunction<MemoryMapState<Long>, Long> {
		@Override
		public List<Long> batchRetrieve(MemoryMapState<Long> state,
				List<TridentTuple> args) {
			List<List<Object>> keys = new ArrayList<List<Object>>();
			for (TridentTuple tuple : args) {
				keys.add(new Values(tuple.getString(0)));
			}
			return state.multiGet(keys);
		}

		@Override
		public void execute(TridentTuple tuple, Long result,
				TridentCollector collector) {
			collector.emit(new Values(result));
		}
	}

	public static class WordCountSpout implements IBatchSpout {

		private static final Fields OUT_FIELDS = new Fields("word");
		private AtomicBoolean sended;
		private AtomicBoolean sended2;
		private AtomicBoolean sended3;

		@Override
		public void open(@SuppressWarnings("rawtypes") Map conf,
				TopologyContext context) {
			sended = new AtomicBoolean(false);
			sended2 = new AtomicBoolean(false);
			sended3 = new AtomicBoolean(false);
		}

		@Override
		public void emitBatch(long batchId, TridentCollector collector) {
			if (sended.compareAndSet(false, true)) {
				List<String> source = new ArrayList<String>();
				source.add("one");
				source.add("two");
				source.add("one");
				for (String item : source) {
					collector.emit(new Values(item));
				}
			} else if (sended2.compareAndSet(false, true)) {
				List<String> source = new ArrayList<String>();
				source.add("two");
				source.add("three");
				for (String item : source) {
					collector.emit(new Values(item));
				}
			} else if (sended3.compareAndSet(false, true)) {
				List<String> source = new ArrayList<String>();
				source.add("one");
				source.add("two");
				for (String item : source) {
					collector.emit(new Values(item));
				}
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
			return OUT_FIELDS;
		}

	}
}
