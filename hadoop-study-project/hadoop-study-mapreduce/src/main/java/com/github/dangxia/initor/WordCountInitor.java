package com.github.dangxia.initor;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.github.dangxia.conf.WordCountConf;
import com.github.dangxia.tool.InitorToolRunner.Initor;
import com.github.dangxia.util.FileSystemUtil;

public class WordCountInitor extends Initor {

	@Override
	public void init() throws IOException {
		FileSystem fs = FileSystem.get(getConf());

		if (fs.exists(WordCountConf.wordCountOutputPath)) {
			fs.delete(WordCountConf.wordCountOutputPath, true);
		}
		if (fs.exists(WordCountConf.sortWordOutputPath)) {
			fs.delete(WordCountConf.sortWordOutputPath, true);
		}
		if (!fs.exists(WordCountConf.wordCountInputPath)) {
			fs.mkdirs(WordCountConf.wordCountInputPath);
		}

		FileSystemUtil.copyFileInJarToHDFS(fs, new Path(
				WordCountConf.WORD_COUNT_INPUT_PATH + "/LICENSE.txt"), true,
				"/LICENSE.txt");
	}

}
