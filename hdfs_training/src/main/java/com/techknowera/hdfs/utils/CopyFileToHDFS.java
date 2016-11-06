package com.techknowera.hdfs.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class CopyFileToHDFS {

	public static void main(String args[]) {

		System.out.println("Accessing the HDFS");
		System.out.println("HDFS file name: " + args[0]);

		FSDataInputStream in = null;
		FSDataOutputStream out = null;

		Configuration conf = new Configuration();
		
		// Hadoop DFS deals with Path
		Path inFile = new Path(args[0]);
		Path outFile = new Path(args[1]);
		

		try {
			FileSystem localfs = FileSystem.getLocal(conf);
			FileSystem fs = FileSystem.get(conf);
		
			// Check if input/output are valid
			if (!fs.exists(inFile))
				System.out.println("Input file not found");
			if (!fs.isFile(inFile))
				System.out.println("Input should be a file");
			if (fs.exists(outFile))
				System.out.println("Output already exists");
			
			// Read file from HDFS
			in = localfs.open(inFile);
			out = fs.create(outFile);
			IOUtils.copyBytes(in, out, 4096);
		} catch (Exception e) {
			System.out.println("Error while copying file");
			System.out.println(e);
		} finally {
			System.out.println("Closing input and output streams");
			IOUtils.closeStream(in);
			IOUtils.closeStream(out);
		}
	}

}
