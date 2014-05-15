package com.github.dangxia.initor;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.github.dangxia.conf.WordCountConf;
import com.github.dangxia.tool.InitorToolRunner.Initor;

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

		copyNovels(fs);
	}

	public void copyNovels(FileSystem fs) throws IllegalArgumentException,
			IOException {
		File inputFile = new File(WordCountInitor.class.getResource("/input")
				.getFile());
		List<Path> paths = new ArrayList<Path>();
		for (File txtFile : inputFile.listFiles()) {
			if (txtFile.isFile() && txtFile.getName().endsWith(".txt")) {
				paths.add(new Path(txtFile.getAbsolutePath()));
			}
		}
		if (paths.size() > 0) {
			fs.copyFromLocalFile(false, true, paths.toArray(new Path[] {}),
					new Path(WordCountConf.WORD_COUNT_INPUT_PATH + "/"));
		}
	}

}
