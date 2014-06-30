/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.github.dangxia.study.storm.state;

import java.util.List;
import java.util.Map;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.Stream;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.fluent.ChainedAggregatorDeclarer;
import storm.trident.fluent.GlobalAggregationScheme;
import storm.trident.fluent.IAggregatableStream;
import storm.trident.operation.CombinerAggregator;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.operation.impl.GlobalBatchToPartition;
import storm.trident.operation.impl.SingleEmitAggregator.BatchToPartition;
import storm.trident.spout.IBatchSpout;
import storm.trident.state.State;
import storm.trident.state.StateFactory;
import storm.trident.state.StateSpec;
import storm.trident.state.StateUpdater;
import storm.trident.tuple.TridentTuple;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.task.IMetricsContext;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

/**
 * @author XuehuiHe
 * @date 2014年6月27日
 */
public class ChainedAggregatorDeclarerTopology {
	private static Logger logger = LoggerFactory
			.getLogger(ChainedAggregatorDeclarerTopology.class);

	public static TridentState combine(Stream stream) {
		return new ChainedAggregatorDeclarer(stream, new GlobalAggScheme())
				.aggregate(IntCombiner.INPUT_FIELDS, new IntCombiner(),
						IntCombiner.OUTPUT_FIELDS)
				.chainEnd()
				.partitionPersist(new StateSpec(new GlobalStateFactory()),
						IntCombiner.OUTPUT_FIELDS, new GlobalStateUpdater(),
						GlobalStateUpdater.OUTPUT_FIELDS);

	}

	public static void main(String[] args) {
		Config config = new Config();
		config.setMaxSpoutPending(1);

		TridentTopology topology = new TridentTopology();
		Stream stream = topology.newStream("jsldjfl", new IntSpout())
				.parallelismHint(2);
		combine(stream);

		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("sdjflsjd", config, topology.build());
	}

	public static class GlobalStateFactory implements StateFactory {
		@Override
		public State makeState(@SuppressWarnings("rawtypes") Map conf,
				IMetricsContext metrics, int partitionIndex, int numPartitions) {
			return new GlobalState();
		}
	}

	public static class GlobalStateUpdater implements StateUpdater<GlobalState> {
		public static final Fields OUTPUT_FIELDS = new Fields("i3");

		@Override
		public void prepare(@SuppressWarnings("rawtypes") Map conf, TridentOperationContext context) {
		}

		@Override
		public void cleanup() {
		}

		@Override
		public void updateState(GlobalState state, List<TridentTuple> tuples,
				TridentCollector collector) {
			if (tuples.size() > 1) {
				throw new RuntimeException("tuples size > 1");
			}
			for (TridentTuple tuple : tuples) {
				state.add((long) tuple.getInteger(0));
			}
		}

	}

	public static class GlobalState implements State {
		private long total;

		public GlobalState() {
		}

		@Override
		public void beginCommit(Long txid) {
		}

		@Override
		public void commit(Long txid) {
		}

		public long add(long i) {
			total += i;
			logger.info("--------------------------------" + total);
			return total;
		}

	}

	public static class IntCombiner implements CombinerAggregator<Integer> {
		public static final Fields INPUT_FIELDS = new Fields("i1");
		public static final Fields OUTPUT_FIELDS = new Fields("i2");

		@Override
		public Integer init(TridentTuple tuple) {
			return tuple.getInteger(0);
		}

		@Override
		public Integer combine(Integer val1, Integer val2) {
			return val1 + val2;
		}

		@Override
		public Integer zero() {
			return 0;
		}
	}

	public static class IntSpout implements IBatchSpout {
		private long total;
		private Random r;

		@Override
		public void open(@SuppressWarnings("rawtypes") Map conf,
				TopologyContext context) {
			r = new Random();
		}

		@Override
		public void emitBatch(long batchId, TridentCollector collector) {
			int times = r.nextInt(3);
			for (int i = 0; i < times; i++) {
				int next = r.nextInt(10);
				total += next;
				collector.emit(new Values(next));
			}
			logger.info("thread_id:" + Thread.currentThread().getId()
					+ "\ttotal:" + total);

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
			return IntCombiner.INPUT_FIELDS;
		}
	}

	static class GlobalAggScheme implements GlobalAggregationScheme<Stream> {

		@Override
		public IAggregatableStream aggPartition(Stream s) {
			return s.global();
		}

		@Override
		public BatchToPartition singleEmitPartitioner() {
			return new GlobalBatchToPartition();
		}

	}
}
