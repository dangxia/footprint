/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.github.dangxia.stream.merge;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.Stream;
import storm.trident.TridentTopology;
import storm.trident.operation.BaseFilter;
import storm.trident.operation.TridentCollector;
import storm.trident.spout.IBatchSpout;
import storm.trident.tuple.TridentTuple;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.GlobalStreamId;
import backtype.storm.grouping.CustomStreamGrouping;
import backtype.storm.task.TopologyContext;
import backtype.storm.task.WorkerTopologyContext;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

/**
 * @author XuehuiHe
 * @date 2014年6月5日
 */
public class StreamMergeTopology {
	private static final Logger log = LoggerFactory
			.getLogger(StreamMergeTopology.class);
	private static final List<String> ONE_SOURCE = new ArrayList<String>();
	private static final List<String> TWO_SOURCE = new ArrayList<String>();
	static {
		ONE_SOURCE.add("one");
		ONE_SOURCE.add("two");
		ONE_SOURCE.add("three");
		ONE_SOURCE.add("four");
		ONE_SOURCE.add("five");
		ONE_SOURCE.add("six");
		ONE_SOURCE.add("seven");
		ONE_SOURCE.add("eight");
		ONE_SOURCE.add("nine");
		ONE_SOURCE.add("ten");
		ONE_SOURCE.add("eleven");

		TWO_SOURCE.add("January");
		TWO_SOURCE.add("February");
		TWO_SOURCE.add("March");
		TWO_SOURCE.add("April");
		TWO_SOURCE.add("May");
		TWO_SOURCE.add("June");
		TWO_SOURCE.add("September");
		TWO_SOURCE.add("October");
		TWO_SOURCE.add("November");
		TWO_SOURCE.add("December");
	}

	public static Config createConfig() {
		Config config = new Config();
		config.setMaxSpoutPending(1);
		config.setNumWorkers(1);
		// config.setDebug(true);
		return config;
	}

	public static void main(String[] args) {
		Config config = createConfig();
		WordSpout spout1 = new WordSpout(ONE_SOURCE, "test1");
		WordSpout spout2 = new WordSpout(TWO_SOURCE, "test2");
		PrintFilter printer = new PrintFilter();
		DelayFilter delayer = new DelayFilter(TimeUnit.SECONDS, 1);

		TridentTopology topology = new TridentTopology();
		Stream stream1 = topology.newStream("test1", spout1).parallelismHint(2)
				.shuffle();
		Stream stream2 = topology.newStream("test2", spout2).parallelismHint(4)
				.shuffle();
		Stream merge = topology.merge(stream1, stream2);
		merge.partition(new MergeCustomStreamGrouping())
				.each(WordSpout.OUT_FIELDS, delayer)
				.each(WordSpout.OUT_FIELDS, printer).parallelismHint(6);

		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("kkkkkk", config, topology.build());
	}

	/**
	 * @author XuehuiHe
	 * @date 2014年6月5日
	 */
	public static final class MergeCustomStreamGrouping implements
			CustomStreamGrouping {

		private List<Integer> targetTasks;
		private int targetTaskSize;
		private Random r;

		@Override
		public void prepare(WorkerTopologyContext context,
				GlobalStreamId stream, List<Integer> targetTasks) {
			this.targetTasks = targetTasks;
			this.targetTaskSize = this.targetTasks.size();
			log.info("----------------------------this stream id:"
					+ stream.get_streamId());
			log.info("----------------------------this component id:"
					+ stream.get_componentId());
			log.info("----------------------------target tasks size:"
					+ targetTaskSize);
			r = new Random();
		}

		@Override
		public List<Integer> chooseTasks(int taskId, List<Object> values) {
			// log.info("this taskId:" + taskId);
			List<Integer> list = new ArrayList<Integer>();
			list.add(targetTasks.get(r.nextInt(targetTaskSize)));
			return list;
		}
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

	public static class WordSpout implements IBatchSpout {
		private final List<String> source;
		private static final Fields OUT_FIELDS = new Fields("word");
		private final String name;

		public WordSpout(List<String> source, String name) {
			this.source = source;
			this.name = name;
		}

		@Override
		public void open(@SuppressWarnings("rawtypes") Map conf,
				TopologyContext context) {

		}

		@Override
		public void emitBatch(long batchId, TridentCollector collector) {
			log.info(name + "\temitBatch into");
			for (String item : source) {
				collector.emit(new Values(item));
			}
			log.info("emitBatch out");
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
