package com.github.dangxia;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;

import com.github.dangxia.chain.SimpleCountSortChain;
import com.github.dangxia.initor.WordCountInitor;
import com.github.dangxia.tool.InitorToolRunner;

public class JobsExecutor extends Configured implements Tool {
	@Override
	public int run(String[] args) throws Exception {
		Job job = JobsFactory.createJob(args[0], getConf());
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		if (args.length < 1) {
			useage();
			return;
		}
		System.setProperty("HADOOP_USER_NAME", "root");
		int ret = 0;
		InitorToolRunner runner = new InitorToolRunner(new WordCountInitor());
		if ("simple-count-sort-chain".equals(args[0])) {
			ret = runner.run(new SimpleCountSortChain(), args);
		} else {
			ret = runner.run(new JobsExecutor(), args);
		}

		System.exit(ret);
	}

	public static void useage() {
		System.out.println("jobs list:");
		System.out.println("\tsimple-count-sort-chain");
		System.out.println("\tsimple-word-count");
		System.out.println("\tsimple-word-count-chain-mapper");
		System.out.println("\tsimple-count-sort");
		System.out.println("\tsimple-file-copy");
	}
}
