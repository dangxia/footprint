package com.github.dangxia.study.storm.state;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.Aggregator;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.spout.IBatchSpout;
import storm.trident.state.QueryFunction;
import storm.trident.testing.MemoryMapState;
import storm.trident.tuple.TridentTuple;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class OemTopology {
	private static Logger logger = LoggerFactory.getLogger(OemTopology.class);

	public static class OemSpout implements IBatchSpout {
		private int times;

		@Override
		public void open(Map conf, TopologyContext context) {
			times = -1;
		}

		@Override
		public void emitBatch(long batchId, TridentCollector collector) {
			if (times < 0) {
				collector.emit(new Values("100"));
			}
			times++;
		}

		@Override
		public void ack(long batchId) {
			logger.info("------------------ack");
		}

		@Override
		public void close() {
			// TODO Auto-generated method stub

		}

		@Override
		public Map getComponentConfiguration() {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public Fields getOutputFields() {
			return new Fields("oemid");
		}

	}

	public static class OemQueryFunction implements
			QueryFunction<MemoryMapState<String>, String> {

		@Override
		public void prepare(Map conf, TridentOperationContext context) {
			// TODO Auto-generated method stub

		}

		@Override
		public void cleanup() {
			// TODO Auto-generated method stub

		}

		@Override
		public List<String> batchRetrieve(MemoryMapState<String> state,
				List<TridentTuple> args) {
			List<List<Object>> keys = new ArrayList<List<Object>>();
			for (TridentTuple tuple : args) {
				keys.add(new Values(tuple.getString(0)));
			}
			return state.multiGet(keys);
		}

		@Override
		public void execute(TridentTuple tuple, String result,
				TridentCollector collector) {
			collector.emit(new Values(result));
		}

	}

	public static class FinalFunction extends BaseFunction {

		@Override
		public void execute(TridentTuple tuple, TridentCollector collector) {
			String oemid = tuple.getStringByField("oemid");
			String spid = tuple.getStringByField("spid");
			System.out.println("--------------oemid:" + oemid + "\tspid:"
					+ spid);
		}

	}

	public static class DrpcFunction extends BaseFunction {

		@Override
		public void execute(TridentTuple tuple, TridentCollector collector) {
			String msg = tuple.getString(0).trim();
			collector.emit(new Values(msg));
		}

	}

	public static void main2(String[] args) {
		Config config = new Config();
		config.setNumWorkers(1);
		// config.setDebug(true);
		config.setMaxTaskParallelism(1);

		TridentTopology topology = new TridentTopology();
		TridentState oemState = topology.newStaticState(new OemStateFactory());
		topology.newStream("spout1", new OemSpout())
				.stateQuery(oemState, new Fields("oemid"),
						new OemQueryFunction(), new Fields("spid"))
				.each(new Fields("oemid", "spid"), new FinalFunction(),
						new Fields());

		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("test", config, topology.build());
	}

	public static class OemAggregator implements Aggregator<String> {

		@Override
		public void prepare(Map conf, TridentOperationContext context) {
			// TODO Auto-generated method stub

		}

		@Override
		public void cleanup() {
			// TODO Auto-generated method stub

		}

		@Override
		public String init(Object batchId, TridentCollector collector) {
			return null;
		}

		@Override
		public void aggregate(String val, TridentTuple tuple,
				TridentCollector collector) {
			if (val == null) {
				collector.emit(new Values(tuple.getStringByField("spid")));
			}
		}

		@Override
		public void complete(String val, TridentCollector collector) {
			collector.emit(new Values(val));
		}

	}

	public static void main(String[] args) {
		Config config = new Config();
		config.setNumWorkers(1);
		// config.setDebug(true);
		config.setMaxTaskParallelism(1);

		LocalCluster cluster = new LocalCluster();
		LocalDRPC drpc = new LocalDRPC();

		TridentTopology topology = new TridentTopology();
		TridentState oemState = topology.newStaticState(new OemStateFactory());
		topology.newDRPCStream("query", drpc)
				.each(new Fields("args"), new DrpcFunction(),
						new Fields("oemid"))
				.stateQuery(oemState, new Fields("oemid"),
						new OemQueryFunction(), new Fields("spid"))
				.each(new Fields("oemid", "spid"), new FinalFunction(),
						new Fields()).groupBy(new Fields("oemid"))
				.aggregate(new OemAggregator(), new Fields("spid2"));
		cluster.submitTopology("test", config, topology.build());

		System.out.println("--------------------"
				+ drpc.execute("query", "101"));
	}
}
