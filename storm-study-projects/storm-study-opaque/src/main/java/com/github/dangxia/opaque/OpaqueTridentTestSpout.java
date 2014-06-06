/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.github.dangxia.opaque;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.TridentTopology;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.spout.IOpaquePartitionedTridentSpout;
import storm.trident.spout.ISpoutPartition;
import storm.trident.topology.TransactionAttempt;
import storm.trident.tuple.TridentTuple;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.task.TopologyContext;
import backtype.storm.transactional.TransactionalTopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import com.github.dangxia.opaque.OpaqueTridentTestSpout.TestSpoutPartition;

/**
 * @author XuehuiHe
 * @date 2014年5月28日
 */
@SuppressWarnings("rawtypes")
public class OpaqueTridentTestSpout
		implements
		IOpaquePartitionedTridentSpout<Integer, TestSpoutPartition, Map<String, String>> {
	private static final Logger logger = LoggerFactory
			.getLogger(OpaqueTridentTestSpout.class);

	public static void info(String msg) {
		logger.info("------------------------------------" + msg);
	}

	@Override
	public Emitter<Integer, TestSpoutPartition, Map<String, String>> getEmitter(
			Map conf, TopologyContext context) {
		info("new emitter");
		return new TestEmitter();
	}

	@Override
	public Coordinator getCoordinator(Map conf, TopologyContext context) {
		info("new Coordinator");
		return new TestCoordinator();
	}

	@Override
	public Map getComponentConfiguration() {
		info("getComponentConfiguration");
		return null;
	}

	@Override
	public Fields getOutputFields() {
		info("getOutputFields");
		return new Fields("word");
	}

	/**
	 * @author XuehuiHe
	 * @date 2014年5月28日
	 */
	private static final class TestBaseFunction extends BaseFunction {
		private static AtomicLong index = new AtomicLong(0);

		@Override
		public void execute(TridentTuple tuple, TridentCollector collector) {
			if (index.get() == 50) {
				info("throw runtime exception");
				index.incrementAndGet();
				throw new RuntimeException("sjdlfjsljdlfjsldjf");
			}
			info("bolt index:" + index.incrementAndGet());
			info("bolt in");
			try {
				TimeUnit.SECONDS.sleep(5);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			info("bolt out");
		}
	}

	class TestEmitter implements
			Emitter<Integer, TestSpoutPartition, Map<String, String>> {
		private Map<Integer, AtomicLong> map = new HashMap<Integer, AtomicLong>();

		@Override
		public Map<String, String> emitPartitionBatch(TransactionAttempt tx,
				TridentCollector collector, TestSpoutPartition partition,
				Map<String, String> lastPartitionMeta) {
			Integer index = partition.getIndex();
			if (!map.containsKey(index)) {
				map.put(index, new AtomicLong(0));
			}
			long running = map.get(index).incrementAndGet();
			info("partition index " + partition.getIndex() + "\trunning:"
					+ running);
			info("txid:" + tx.getTransactionId());
			// for (int i = 0; i < index; i++) {
			collector.emit(new Values("test-word"));
			// }

			return null;
		}

		@Override
		public void refreshPartitions(
				List<TestSpoutPartition> partitionResponsibilities) {
			info("refreshPartitions");
		}

		@Override
		public List<TestSpoutPartition> getOrderedPartitions(
				Integer allPartitionInfo) {
			info("getOrderedPartitions");
			List<TestSpoutPartition> list = new ArrayList<TestSpoutPartition>();
			for (int i = 0; i < allPartitionInfo; i++) {
				TestSpoutPartition t = new TestSpoutPartition();
				t.setIndex(i);
				list.add(t);
			}
			return list;
		}

		@Override
		public void close() {
			info("close");
		}

	}

	class TestCoordinator implements Coordinator<Integer> {
		private int times = 0;

		@Override
		public boolean isReady(long txid) {
			info("isReady");
			return true;
		}

		@Override
		public Integer getPartitionsForBatch() {
			info("getPartitionsForBatch");
			times++;
			if (times < 6) {
				return 4;
			}
			return 8;
		}

		@Override
		public void close() {
			info(" Coordinator close");
		}

	}

	public static class TestSpoutPartition implements ISpoutPartition {
		private int index;

		public int getIndex() {
			return index;
		}

		public void setIndex(int index) {
			this.index = index;
		}

		@Override
		public String getId() {
			return index + "";
		}

	}

	public static void main(String[] args) throws InterruptedException {
		Config config = new Config();
		config.setMaxSpoutPending(1);
		config.setMessageTimeoutSecs(120);
		config.setNumWorkers(1);

		LocalCluster cluster = new LocalCluster();

		TridentTopology topology = new TridentTopology();
		topology.newStream("test-opaque", new OpaqueTridentTestSpout())
				// .parallelismHint(1)
				.each(new Fields("word"), new TestBaseFunction(), new Fields())
				.parallelismHint(5);

		cluster.submitTopology("test-opaue-topology", config, topology.build());

		TimeUnit.SECONDS.sleep(1000);
		cluster.killTopology("test-opaue-topology");
		cluster.shutdown();

	}
}
