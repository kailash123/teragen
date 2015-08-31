package org.myprogram;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import javax.xml.soap.Text;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;

public class DataExtrapolatorDriver {
	
	private static final String DICTIONARY_TOTAL_TUPLE = "dictionary.total.tuple";
	private static final String MULTIPLICATION_FACTOR = "extrapolated.tuple.mulitplication.factor";
	private static final String NUM_MAP_TASKS = "extrapolated.map.tasks";

	/**
	 * For executing data extrapolation jar user needs to fire command like this
	 * bin/hadoop jar dataextrapolator.jar com.impetus.datagenerator.DataExtrapolatorDriver
	 *  /home/impadmin/Documents/anycompanydata/zipdata.csv /user/teragen/dictionary/mergedata.csv 
	 *  1 
	 *  /user/teragen/dictionary/result1
	 * 
	 */
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException, URISyntaxException {
		long sizeToCreate = 0L;
		long oneGBBytes = 1 * 1024 *1024 * 1024;
		long totalTuple = 0L;
		long factorToMultiplyFile = 0L;
		long length = 0L; 
		String dictonaryPathInHDFS = null;
		String filePathInLocalFileSystem = null;

		
		//getting arguments
		filePathInLocalFileSystem = args[0];
		dictonaryPathInHDFS = args[1];
		sizeToCreate = Integer.parseInt(args[2]);
		String outputDirectoryPath = args[3];
		
		length = getTotalFileSize(filePathInLocalFileSystem);
		totalTuple = getLineCount(filePathInLocalFileSystem);
		factorToMultiplyFile = oneGBBytes/length;
		
		Path outputDir = new Path(outputDirectoryPath);
		
		Configuration config = new Configuration();
		DistributedCache.addCacheFile(new URI(dictonaryPathInHDFS), config);
		Job job = new Job(config, "ExtrapolateData");
		job.setJarByClass(DataExtrapolatorRecordReader.class);
		
		//URI path  = DistributedCache.getCacheFiles(config)[0];
		job.getConfiguration().setLong(DICTIONARY_TOTAL_TUPLE, totalTuple);
		job.getConfiguration().setLong(MULTIPLICATION_FACTOR, factorToMultiplyFile);
		job.getConfiguration().setLong(NUM_MAP_TASKS, sizeToCreate);
		job.setNumReduceTasks(0);
		job.setInputFormatClass(ExtrapolateInputFormatGenerator.class);
		
		org.apache.hadoop.mapreduce.lib.output.TextOutputFormat.setOutputPath(job, outputDir);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		System.exit(job.waitForCompletion(true) ? 0 : 2);

		
		
	}

	

	 /**
	 * Counts the line in a file separated by "\n"
	 */
	 private static long getLineCount(String filePathInLocalFileSystem) throws
	 IOException {
	 BufferedReader bufferedReader = new BufferedReader(new FileReader(new
	 File(filePathInLocalFileSystem)));
	 long count = 0;
	 while(bufferedReader.readLine() !=null){
	 count++;
	 }
	 bufferedReader.close();
	 return count;
	 }

	private static long getTotalFileSize(String filePathInLocalFileSystem) {
		long length = new File(filePathInLocalFileSystem).length();
		return length;

	}
}
