package com.github.dangxia.study.storm;

import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class MaxPendingTopology {

	private static Logger logger = LoggerFactory
			.getLogger(MaxPendingTopology.class);

	public static class LimitPendingSpout extends BaseRichSpout {

		private SpoutOutputCollector collector;
		private List<String> data;
		private Random r;
		private int size;
		private int msgId = 0;

		private AtomicInteger running = new AtomicInteger(0);

		@SuppressWarnings("rawtypes")
		@Override
		public void open(Map conf, TopologyContext context,
				SpoutOutputCollector collector) {
			this.collector = collector;
			this.size = 50;
			this.data = StormUtil.getRandomStrs(this.size);
			this.r = new Random();
		}

		@Override
		public void nextTuple() {
			collector.emit(new Values(data.get(r.nextInt(this.size))), msgId);
			msgId++;
			int now = running.incrementAndGet();
			logger.info("spout emit one,running:" + now);
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("word"));
		}

		@Override
		public void ack(Object msgId) {
			super.ack(msgId);
			int now = running.decrementAndGet();
			logger.info("spout ack one,running:" + now);
		}

		@Override
		public void fail(Object msgId) {
			super.fail(msgId);
			int now = running.decrementAndGet();
			logger.info("spout fail one,running:" + now);
		}
	}

	public static class FinalBolt extends BaseRichBolt {

		@Override
		public void prepare(Map stormConf, TopologyContext context,
				OutputCollector collector) {
			// TODO Auto-generated method stub

		}

		@Override
		public void execute(Tuple input) {
			// TODO Auto-generated method stub

		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			// TODO Auto-generated method stub

		}

	}

	public static class DelayBolt2 implements IBasicBolt {
		private int maxSeconds;

		public DelayBolt2() {
			this(5);
		}

		public DelayBolt2(int maxSeconds) {
			this.maxSeconds = maxSeconds;
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("word2"));
		}

		@Override
		public Map<String, Object> getComponentConfiguration() {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public void prepare(Map stormConf, TopologyContext context) {
			// TODO Auto-generated method stub

		}

		@Override
		public void execute(Tuple input, BasicOutputCollector collector) {
			try {
				int seconds = new Random().nextInt(this.maxSeconds);
				logger.info("bolt sleep " + seconds + " seconds");
				TimeUnit.SECONDS.sleep(seconds);
				collector.emit(new Values(input.getString(0)));
				logger.info("bolt wakeup ");
			} catch (InterruptedException e) {
				e.printStackTrace();
			}

		}

		@Override
		public void cleanup() {

		}

	}

	public static class DelayBolt extends BaseRichBolt {
		private int maxSeconds;
		private OutputCollector collector;

		public DelayBolt() {
			this(5);
		}

		public DelayBolt(int maxSeconds) {
			this.maxSeconds = maxSeconds;
		}

		@Override
		public void prepare(Map stormConf, TopologyContext context,
				OutputCollector collector) {
			this.collector = collector;

		}

		@Override
		public void execute(Tuple input) {
			try {
				int seconds = new Random().nextInt(this.maxSeconds);
				logger.info("bolt sleep " + seconds + " seconds");
				TimeUnit.SECONDS.sleep(seconds);
				collector.emit(input, new Values(input.getString(0)));
				collector.ack(input);
				logger.info("bolt wakeup ");
			} catch (InterruptedException e) {
				e.printStackTrace();
			}

		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("word2"));
		}

	}

	public static void main(String[] args) throws InterruptedException {

		int maxSeconds = 15;
		int messageTimeOut = maxSeconds + 10;

		Config config = new Config();
		config.setMaxSpoutPending(5);
		config.setMessageTimeoutSecs(messageTimeOut);
		config.setMaxTaskParallelism(1);
		config.setNumWorkers(1);
		config.setNumAckers(1);
		// config.setDebug(true);

		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("LimitPendingSpout", new LimitPendingSpout(), 1);
		builder.setBolt("DelayBolt", new DelayBolt2(maxSeconds), 5)
				.shuffleGrouping("LimitPendingSpout");
		builder.setBolt("FinalBolt", new FinalBolt()).shuffleGrouping(
				"DelayBolt");
		LocalCluster local = new LocalCluster();
		local.submitTopology("test-max-pending", config,
				builder.createTopology());

		TimeUnit.SECONDS.sleep(100);
		local.killTopology("test-max-pending");
		local.shutdown();

	}
}
