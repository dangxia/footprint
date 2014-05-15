package com.github.dangxia.reducer;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SimpleCountSortReducer extends
		Reducer<IntWritable, Text, IntWritable, Text> {
	private static IntWritable linenum = new IntWritable(1);

	@Override
	protected void reduce(IntWritable key, Iterable<Text> values,
			Context context) throws IOException, InterruptedException {
		for (Text text : values) {
			context.write(linenum, text);
		}
		linenum = new IntWritable(linenum.get() + 1);
	}
}