package com.github.dangxia.study.storm.state;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.Map.Entry;

import org.springframework.context.support.ClassPathXmlApplicationContext;

import storm.trident.state.State;
import storm.trident.state.StateFactory;
import storm.trident.testing.MemoryMapState;
import backtype.storm.task.IMetricsContext;
import backtype.storm.tuple.Values;

public class OemStateFactory implements StateFactory {

	private static final long serialVersionUID = 4141103240925608069L;

	String _id;

	public OemStateFactory() {
		_id = UUID.randomUUID().toString();
	}

	@Override
	public State makeState(Map conf, IMetricsContext metrics,
			int partitionIndex, int numPartitions) {
		MemoryMapState<String> state = new MemoryMapState<String>(_id
				+ partitionIndex);
		init(state);
		return state;
	}

	private void init(MemoryMapState<String> state) {
		ClassPathXmlApplicationContext cxt = new ClassPathXmlApplicationContext(
				"spring-state.xml");
		OemDao dao = cxt.getBean(OemDao.class);
		Map<String, String> map = dao.getOemidToSpid();
		List<List<Object>> keys = new ArrayList<List<Object>>();
		List<String> values = new ArrayList<String>();
		for (Entry<String, String> entry : map.entrySet()) {
			keys.add(new Values(entry.getKey()));
			values.add(entry.getValue());
		}
		cxt.close();

		state.multiPut(keys, values);
	}
}
