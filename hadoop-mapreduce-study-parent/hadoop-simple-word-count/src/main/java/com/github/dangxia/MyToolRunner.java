package com.github.dangxia;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MyToolRunner {
	public static final String WORD_COUNT_INPUT_PATH = "/hexh/simple-word-count/input/";
	public static final String WORD_COUNT_OUTPUT_PATH = "/hexh/simple-word-count/output/";
	public static final String SORT_WORD_OUTPUT_PATH = "/hexh/simple-sort/output/";
	private static final String INTPU_FILE_TMP = "/tmp/simple-word-count.tmp";

	public static final Path wordCountInputPath = new Path(
			WORD_COUNT_INPUT_PATH);
	public static final Path wordCountOutputPath = new Path(
			WORD_COUNT_OUTPUT_PATH);
	public static final Path sortWordOutputPath = new Path(
			SORT_WORD_OUTPUT_PATH);

	public static int run(Tool tool, String[] args) throws Exception {
		return ToolRunner.run(new ToolSwapper(tool), args);
	}

	public static void init(FileSystem fs) throws IOException {
		if (fs.exists(wordCountOutputPath)) {
			fs.delete(wordCountOutputPath, true);
		}
		if (fs.exists(sortWordOutputPath)) {
			fs.delete(sortWordOutputPath, true);
		}
		if (!fs.exists(wordCountInputPath)) {
			fs.mkdirs(wordCountInputPath);
		}
	}

	public static void readyWordCountInput(FileSystem fs) throws IOException {
		copyJarFileToTmp();
		fs.copyFromLocalFile(true, true, new Path(INTPU_FILE_TMP), new Path(
				WORD_COUNT_INPUT_PATH + "/LICENSE.txt"));
	}

	private static void copyJarFileToTmp() {
		InputStream inputStream = MyToolRunner.class
				.getResourceAsStream("/LICENSE.txt");
		OutputStream outputStream = null;
		try {
			outputStream = new FileOutputStream(new File(INTPU_FILE_TMP));

			int read = 0;
			byte[] bytes = new byte[1024];

			while ((read = inputStream.read(bytes)) != -1) {
				outputStream.write(bytes, 0, read);
			}

		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (inputStream != null) {
				try {
					inputStream.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			if (outputStream != null) {
				try {
					// outputStream.flush();
					outputStream.close();
				} catch (IOException e) {
					e.printStackTrace();
				}

			}
		}
	}

	private static class ToolSwapper implements Tool {
		private final Tool inner;

		public ToolSwapper(Tool tool) {
			inner = tool;
		}

		@Override
		public void setConf(Configuration conf) {
			inner.setConf(conf);
		}

		@Override
		public Configuration getConf() {
			return inner.getConf();
		}

		@Override
		public int run(String[] args) throws Exception {
			FileSystem fs = FileSystem.get(getConf());
			init(fs);
			readyWordCountInput(fs);
			return inner.run(args);
		}

	}

}
