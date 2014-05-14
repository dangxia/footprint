package com.github.dangxia.mapper;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends
		Mapper<LongWritable, Text, Text, IntWritable> {
	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		StringTokenizer itr = new StringTokenizer(line);
		while (itr.hasMoreTokens()) {
			String str = itr.nextToken().replaceAll("[,.\"()!*/]", "");
			if (str.length() > 0) {
				word.set(str);
				context.write(word, one);
			}
		}
	}
}