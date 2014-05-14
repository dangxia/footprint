package com.github.dangxia;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;

import com.github.dangxia.conf.WordCountConf;
import com.github.dangxia.initor.WordCountInitor;
import com.github.dangxia.mapper.WordCountMapper;
import com.github.dangxia.reducer.WordCountReducer;
import com.github.dangxia.tool.InitorToolRunner;

public class SimpleWordCount extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		return createJob(getConf()).waitForCompletion(true) ? 0 : 1;
	}

	public static Job createJob(Configuration config) throws Exception {
		Job job = Job.getInstance(config);
		job.setJobName("simple-word-count");
		job.setJarByClass(SimpleWordCount.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapperClass(WordCountMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setCombinerClass(WordCountReducer.class);

		job.setReducerClass(WordCountReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPaths(job, WordCountConf.WORD_COUNT_INPUT_PATH);
		FileOutputFormat.setOutputPath(job, new Path(
				WordCountConf.WORD_COUNT_OUTPUT_PATH));

		return job;
	}

	public static String getOutputDir() {
		return "/hexh/simple-word-count/output";
	}

	public static String getInputDir() {
		return "/hexh/simple-word-count/input";
	}

	public static void main(String[] args) throws Exception {
		int ret = new InitorToolRunner(new WordCountInitor()).run(
				new SimpleWordCount(), args);
		System.exit(ret);
	}

}
