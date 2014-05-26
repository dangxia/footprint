package com.github.dangxia.study.storm.state;

import storm.trident.TridentTopology;
import storm.trident.testing.MemoryMapState;

public class Testsdlfj<T> extends MemoryMapState<T> {

	public Testsdlfj(String id) {
		super(id);

		TridentTopology topology = new TridentTopology();
		// topology.newStaticState(factory)
	}

}
