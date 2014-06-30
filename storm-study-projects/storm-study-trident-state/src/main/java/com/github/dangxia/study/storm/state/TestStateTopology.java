/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.github.dangxia.study.storm.state;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.task.IMetricsContext;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.TridentCollector;
import storm.trident.spout.IBatchSpout;
import storm.trident.state.BaseQueryFunction;
import storm.trident.state.State;
import storm.trident.state.StateFactory;
import storm.trident.tuple.TridentTuple;

/**
 * @author XuehuiHe
 * @date 2014年6月27日
 */
public class TestStateTopology {
	private static Logger logger = LoggerFactory
			.getLogger(TestStateTopology.class);

	public static class TestState implements State {
		@Override
		public void beginCommit(Long txid) {
		}

		@Override
		public void commit(Long txid) {
		}

		public String get(int key) {
			logger.info(Thread.currentThread().getId()
					+ "\tTestState get invoke");
			return "test-state";
		}
	}

	public static class TestStateFactory implements StateFactory {

		@Override
		public State makeState(Map conf, IMetricsContext metrics,
				int partitionIndex, int numPartitions) {
			return new TestState();
		}

	}

	public static class TestStateQueryFunction extends
			BaseQueryFunction<TestState, String> {
		public static final Fields INPUT_FIELDS = new Fields("oemid");
		public static final Fields OUTPUT_FIELDS = new Fields("oemid2");

		@Override
		public List<String> batchRetrieve(TestState state,
				List<TridentTuple> args) {
			List<String> list = new ArrayList<String>();
			for (TridentTuple tuple : args) {
				list.add(state.get(tuple.getInteger(0)));
			}
			return list;
		}

		@Override
		public void execute(TridentTuple tuple, String result,
				TridentCollector collector) {
		}

	}

	public static class OemSpout implements IBatchSpout {
		@Override
		public void open(@SuppressWarnings("rawtypes") Map conf,
				TopologyContext context) {
		}

		@Override
		public void emitBatch(long batchId, TridentCollector collector) {
			logger.info(Thread.currentThread().getId() + "\tjsdljflsjdf");
			collector.emit(new Values(1));
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
			return TestStateQueryFunction.INPUT_FIELDS;
		}
	}

	public static void main(String[] args) {

		Config config = new Config();
		config.setMaxSpoutPending(1);

		TridentTopology topology = new TridentTopology();
		TridentState state = topology.newStaticState(new TestStateFactory())
				.parallelismHint(4);
		topology.newStream("jsldjfl", new OemSpout())
				.shuffle()
				.stateQuery(state, TestStateQueryFunction.INPUT_FIELDS,
						new TestStateQueryFunction(),
						TestStateQueryFunction.OUTPUT_FIELDS);

		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("sdjflsjd", config, topology.build());
	}
}
