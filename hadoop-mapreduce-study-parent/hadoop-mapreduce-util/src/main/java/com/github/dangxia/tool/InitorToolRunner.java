package com.github.dangxia.tool;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class InitorToolRunner {
	private final Initor initor;

	public InitorToolRunner(Initor initor) {
		this.initor = initor;
	}

	public int run(Tool tool, String[] args) throws Exception {
		initor.setInner(tool);
		return ToolRunner.run(initor, args);
	}

	public Initor getInitor() {
		return initor;
	}

	public abstract static class Initor implements Tool {
		private Tool inner;

		public abstract void init() throws IOException;

		public Initor() {
		}

		@Override
		public Configuration getConf() {
			return inner.getConf();
		}

		@Override
		public void setConf(Configuration conf) {
			inner.setConf(conf);
		}

		@Override
		public int run(String[] args) throws Exception {
			init();
			return inner.run(args);
		}

		public Tool getInner() {
			return inner;
		}

		public void setInner(Tool inner) {
			this.inner = inner;
		}

	}
}
