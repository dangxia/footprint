package com.github.dangxia;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.github.dangxia.mapper.SimpleSortMapper;
import com.github.dangxia.reducer.SimpleSortReducer;

public class SimpleSort extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		return createJob(getConf()).waitForCompletion(true) ? 0 : 1;
	}

	public static Job createJob(Configuration config) throws Exception {
		Job job = Job.getInstance(config);
		job.setJobName("simple-sort");
		job.setJarByClass(SimpleSort.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapperClass(SimpleSortMapper.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);

		// job.setCombinerClass(WordCountReducer.class);

		job.setReducerClass(SimpleSortReducer.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);

		FileSystem fs = FileSystem.get(job.getConfiguration());
		Path outputPath = new Path(getOutputDir());
		if (fs.exists(outputPath)) {
			fs.delete(outputPath, true);
		}
		FileInputFormat.addInputPaths(job, getInputDir());
		FileOutputFormat.setOutputPath(job, new Path(getOutputDir()));
		return job;
	}

	public static String getOutputDir() {
		return "/hexh/simple-sort/output";
	}

	private static String getInputDir() {
		return "/hexh/simple-word-count/output";
	}

	public static void main(String[] args) throws Exception {
		int ret = ToolRunner.run(new SimpleSort(), args);
		System.exit(ret);
	}

}
