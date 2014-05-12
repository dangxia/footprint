package com.github.dangxia;

import java.io.File;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SimpleWordCount extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		if (args.length < 2) {
			return 2;
		}
		Job job = Job.getInstance(getConf());
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

		FileInputFormat.addInputPaths(job, getInputDir(args));
		FileOutputFormat.setOutputPath(job, new Path(getOutputDir(args)));

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static class WordCountMapper extends
			Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			StringTokenizer itr = new StringTokenizer(line);
			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken());
				context.write(word, one);
			}
		}
	}

	public static class WordCountReducer extends
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

	public static String getResourcePath() {
		File classPath = new File(SimpleWordCount.class.getResource("/")
				.getFile());
		return classPath.getParentFile().getParentFile().getAbsolutePath()
				+ "/src/main/resources";
	}

	public static String getOutputDir(String... args) {
		// return getResourcePath() + "/output";
		return "/hexh/simple-word-count/output";
	}

	private static String getInputDir(String... args) {
		// return getResourcePath() + "/input";
		return "/hexh/simple-word-count/input";
	}

	public static void cleanOutPut() {
		String outputDir = getOutputDir();
		File file = new File(outputDir);
		if (file.exists()) {
			System.out.println(file.delete());
			// file.delete();
		}
	}

	public static void main(String[] args) throws Exception {
		int ret = ToolRunner.run(new SimpleWordCount(), args);
		System.exit(ret);
	}

}
