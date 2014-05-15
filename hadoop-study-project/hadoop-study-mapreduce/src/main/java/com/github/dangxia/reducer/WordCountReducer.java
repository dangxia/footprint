package com.github.dangxia.reducer;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReducer extends
		Reducer<Text, IntWritable, Text, IntWritable> {
	private IntWritable total = new IntWritable();

	protected void reduce(Text text, Iterable<IntWritable> iterable,
			Context context) throws IOException, InterruptedException {
		int sum = 0;
		for (IntWritable intWritable : iterable) {
			sum += intWritable.get();
		}
		total.set(sum);
		context.write(text, total);
	};
}