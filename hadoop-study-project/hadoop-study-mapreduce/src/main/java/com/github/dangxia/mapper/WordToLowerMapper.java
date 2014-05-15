package com.github.dangxia.mapper;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordToLowerMapper extends
		Mapper<Text, IntWritable, Text, IntWritable> {
	private Text text = new Text();

	@Override
	protected void map(Text key, IntWritable value, Context context)
			throws IOException, InterruptedException {
		if (key == null) {
			return;
		}
		text.set(new String(key.getBytes(), "UTF-8").toLowerCase());
		context.write(text, value);
	}
}
