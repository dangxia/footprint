package com.github.dangxia;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;

import com.github.dangxia.mapper.WordCountMapper;
import com.github.dangxia.reducer.WordCountReducer;

public class SimpleChainMapper extends SimpleWordCount {
	public int run(String[] args) throws Exception {
		return createJob(getConf()).waitForCompletion(true) ? 0 : 1;
	}

	public static Job createJob(Configuration config) throws Exception {
		Job job = Job.getInstance(config);
		job.setJobName("simple-word-count");
		job.setJarByClass(SimpleWordCount.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		ChainMapper.addMapper(job, WordCountMapper.class, LongWritable.class,
				Text.class, Text.class, IntWritable.class, new Configuration(
						false));

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

		FileSystem fs = FileSystem.get(job.getConfiguration());
		Path outputPath = new Path(getOutputDir());
		if (fs.exists(outputPath)) {
			fs.delete(outputPath, true);
		}

		FileInputFormat.addInputPaths(job, getInputDir());
		FileOutputFormat.setOutputPath(job, new Path(getOutputDir()));

		return job;
	}

	public static class WordTheFilterMapper extends
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

	public static void main(String[] args) throws Exception {
		int ret = ToolRunner.run(new SimpleChainMapper(), args);
		System.exit(ret);
	}
}
