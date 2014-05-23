package com.github.dangxia.proto;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.github.dangxia.proto.PersonPB.Person;

public class ProtoBuffSerializerToplogy {
	public static class ProtoBuffSerializerSpout extends BaseRichSpout {
		private SpoutOutputCollector _collector;
		private List<Person> persons;
		private Random r;
		private int size;

		@SuppressWarnings("rawtypes")
		@Override
		public void open(Map conf, TopologyContext context,
				SpoutOutputCollector collector) {
			_collector = collector;
			r = new Random();
			persons = new ArrayList<PersonPB.Person>();
			initPersons();
			size = persons.size();
		}

		private void initPersons() {
			for (int i = 0; i < 50; i++) {
				persons.add(Person.newBuilder().setEmail(getEmail())
						.setId(getId()).setName(getName()).build());
			}
		}

		private int getId() {
			return r.nextInt();
		}

		private String getEmail() {
			return "email@" + new Double(r.nextDouble()).toString();
		}

		private String getName() {
			return "name:" + new Double(r.nextDouble()).toString();
		}

		@Override
		public void nextTuple() {
			_collector.emit(new Values(persons.get(r.nextInt(size))));
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("person"));
		}

	}

	public static class ProtoBuffSerializerBolt extends BaseRichBolt {

		@SuppressWarnings("rawtypes")
		@Override
		public void prepare(Map stormConf, TopologyContext context,
				OutputCollector collector) {

		}

		@Override
		public void execute(Tuple input) {
			Person p = (Person) input.getValue(0);
			System.out.println(p.getName());
			System.out.println(p.getEmail());
			System.out.println(p.getId());
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			// TODO Auto-generated method stub

		}

	}

	public static void main(String[] args) throws AlreadyAliveException,
			InvalidTopologyException, InterruptedException {
		Config conf = new Config();
		conf.registerSerialization(Person.class, ProtoBuffSerializer.class);
		conf.setMaxSpoutPending(5);
		conf.setMaxTaskParallelism(2);
		conf.setNumWorkers(3);

		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("test-s-spout", new ProtoBuffSerializerSpout(), 1);
		builder.setBolt("test-s-bolt", new ProtoBuffSerializerBolt(), 1)
				.shuffleGrouping("test-s-spout");

		if (args.length == 0) {
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("test-s", conf, builder.createTopology());
			TimeUnit.SECONDS.sleep(20);

			cluster.killTopology("test-s");
			cluster.shutdown();
		} else {
			StormSubmitter.submitTopology("test-s", conf,
					builder.createTopology());
		}

	}
}
