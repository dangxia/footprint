package com.github.dangxia.util;

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class FileSystemUtil {
	public static void copyFileInJarToHDFS(FileSystem fs, Path target,
			boolean overwrite, String sourceInJar) throws IOException {
		InputStream inputStream = null;
		FSDataOutputStream outputStream = null;

		try {
			outputStream = fs.create(target, overwrite);
			inputStream = FileSystemUtil.class.getResourceAsStream(sourceInJar);
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
}
