package com.github.dangxia.mapper;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SimpleCountSortMapper extends
		Mapper<LongWritable, Text, IntWritable, Text> {
	private IntWritable one = new IntWritable(1);
	private Text word = new Text();

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		StringTokenizer itr = new StringTokenizer(line);
		if (itr.countTokens() != 2) {
			return;
		}
		itr.nextToken();
		String weigthStr = itr.nextToken();
		Integer weight = null;
		try {
			weight = Integer.parseInt(weigthStr);
		} catch (Exception e) {
		}
		if (weight == null) {
			return;
		}
		one.set(weight);
		word.set(value);
		context.write(one, word);
	}
}