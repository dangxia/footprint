package com.github.dangxia;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.github.dangxia.util.FileSystemUtil;

public class ContentSummaryTest {
	public static void main(String[] args) throws Exception {
		FileSystem fs = FileSystemUtil.getFileSystem();
		fs.rename(new Path("/tmp/hexh/test-makdir/"), new Path(
				"/tmp/hexh/test-mkdir/"));
		Path myPath = new Path("/tmp/hexh/");
		FileStatus[] executions = fs.listStatus(myPath);
		for (FileStatus fileStatus : executions) {
			System.out.println(fileStatus);
		}
	}

}
