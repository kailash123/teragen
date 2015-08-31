package org.myprogram;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class DataExtrapolatorRecordReader extends RecordReader<Text, NullWritable>{
	
	private static final String DICTIONARY_TOTAL_TUPLE = "dictionary.total.tuple";
	private static final String MULTIPLICATION_FACTOR = "extrapolated.tuple.mulitplication.factor";
	
	
	private BufferedReader br = null;
	private String currentTuple = null;
	private int currentMultiplicationFrequencyCount = 0;
	private long currentTupleCount = 0;
	private long totalTuple = 0L;
	
	
	private int recordMultiplicationFactor = 0;
	private Text key = new Text();
	private NullWritable value = NullWritable.get();

	

	@Override
	public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
			throws IOException, InterruptedException {
		Path dictionaryHDFSCachePath = DistributedCache.getLocalCacheFiles(taskAttemptContext.getConfiguration())[0];
		
		br = new BufferedReader(new FileReader(new File(dictionaryHDFSCachePath.toUri().getPath())));
		recordMultiplicationFactor =  taskAttemptContext.getConfiguration().getInt(MULTIPLICATION_FACTOR, -1);
		totalTuple = taskAttemptContext.getConfiguration().getLong(DICTIONARY_TOTAL_TUPLE, -1);
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if( !(currentTupleCount<totalTuple )){
			return false;
		}
		if(currentTupleCount==0){
			currentTuple = br.readLine().trim(); 
		}
		if(! (currentMultiplicationFrequencyCount < recordMultiplicationFactor)){
			currentTuple = br.readLine();
			if(currentTuple==null){
				return false;
			}
			currentTuple = currentTuple.trim();
			currentMultiplicationFrequencyCount = 0;
			currentTupleCount++;
		} 
		key.set(currentTuple);
		currentMultiplicationFrequencyCount++;
		return true;
	}
	
	@Override
	public void close() throws IOException {
		
		//closing buffered reader instance
		if(br!=null){
			br.close();
		}
		
	}
	
	@Override
	public Text getCurrentKey() throws IOException, InterruptedException {
		return key;
	}

	@Override
	public NullWritable getCurrentValue() throws IOException,
			InterruptedException {
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return (float)currentTupleCount/ (float)totalTuple;
	}



}
