package com.github.dangxia.mapper;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordTheFilterMapper extends
		Mapper<Text, IntWritable, Text, IntWritable> {
	@Override
	protected void map(Text key, IntWritable value, Context context)
			throws IOException, InterruptedException {
		if (key == null || key.equals(new Text(""))
				|| key.equals(new Text("the"))) {
			return;
		}
		context.write(key, value);
	}
}
