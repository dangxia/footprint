package com.github.dangxia.conf;

import org.apache.hadoop.fs.Path;

public class WordCountConf {
	public static final String WORD_COUNT_INPUT_PATH = "/hexh/simple-word-count/input/";
	public static final String WORD_COUNT_OUTPUT_PATH = "/hexh/simple-word-count/output/";
	public static final String SORT_WORD_OUTPUT_PATH = "/hexh/simple-sort/output/";

	public static final Path wordCountInputPath = new Path(
			WORD_COUNT_INPUT_PATH);
	public static final Path wordCountOutputPath = new Path(
			WORD_COUNT_OUTPUT_PATH);
	public static final Path sortWordOutputPath = new Path(
			SORT_WORD_OUTPUT_PATH);
}
