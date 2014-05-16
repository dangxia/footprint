package com.github.dangxia;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.github.dangxia.conf.WordCountConf;
import com.github.dangxia.mapper.SimpleCountSortMapper;
import com.github.dangxia.mapper.WordCountMapper;
import com.github.dangxia.mapper.WordTheFilterMapper;
import com.github.dangxia.mapper.WordToLowerMapper;
import com.github.dangxia.reducer.SimpleCountSortReducer;
import com.github.dangxia.reducer.WordCountReducer;

public class JobsFactory {
	public static Job createJob(String name, Configuration config)
			throws IOException {
		if (name == null || name.length() == 0) {
			return null;
		} else if ("simple-word-count".equals(name)) {
			return createSimpleWordCount(config);
		} else if ("simple-word-count-chain-mapper".equals(name)) {
			return createSimpleWordCountChainMapper(config);
		} else if ("simple-count-sort".equals(name)) {
			return createSimpleCountSort(config);
		} else if ("simple-file-copy".equals(name)) {
			return createSimpleFileCopy(config);
		}
		return null;
	}

	public static Job createSimpleFileCopy(Configuration config)
			throws IOException {
		Job job = Job.getInstance(config);
		job.setJobName("simple-file-copy");
		job.setJarByClass(JobsFactory.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setNumReduceTasks(0);

		FileInputFormat.addInputPaths(job, WordCountConf.WORD_COUNT_INPUT_PATH);
		FileOutputFormat.setOutputPath(job, new Path(
				WordCountConf.WORD_COUNT_OUTPUT_PATH));

		return job;
	}

	public static Job createSimpleWordCount(Configuration config)
			throws IOException {
		Job job = Job.getInstance(config);
		job.setJobName("simple-word-count");
		job.setJarByClass(JobsFactory.class);

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

	public static Job createSimpleWordCountChainMapper(Configuration config)
			throws IOException {
		Job job = Job.getInstance(config);
		job.setJobName("simple-word-count-chain-mapper");
		job.setJarByClass(JobsFactory.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		ChainMapper.addMapper(job, WordCountMapper.class, LongWritable.class,
				Text.class, Text.class, IntWritable.class, new Configuration(
						false));
		ChainMapper.addMapper(job, WordToLowerMapper.class, Text.class,
				IntWritable.class, Text.class, IntWritable.class,
				new Configuration(false));
		ChainMapper.addMapper(job, WordTheFilterMapper.class, Text.class,
				IntWritable.class, Text.class, IntWritable.class,
				new Configuration(false));

		// job.setMapperClass(WordCountMapper.class);
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

	public static Job createSimpleCountSort(Configuration config)
			throws IOException {
		Job job = Job.getInstance(config);
		job.setJobName("simple-count-sort");
		job.setJarByClass(JobsFactory.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapperClass(SimpleCountSortMapper.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);

		job.setReducerClass(SimpleCountSortReducer.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat
				.addInputPaths(job, WordCountConf.WORD_COUNT_OUTPUT_PATH);
		FileOutputFormat.setOutputPath(job, WordCountConf.sortWordOutputPath);
		return job;
	}

}
