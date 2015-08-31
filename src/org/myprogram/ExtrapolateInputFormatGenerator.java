package org.myprogram;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class ExtrapolateInputFormatGenerator extends InputFormat<Text, NullWritable> {
	private static final String NUM_MAP_TASKS = "extrapolated.map.tasks";

	@Override
	public RecordReader<Text, NullWritable> createRecordReader(InputSplit inputSplit,
			TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
		DataExtrapolatorRecordReader dataExtrapolatorRecordReader = new DataExtrapolatorRecordReader();
		dataExtrapolatorRecordReader.initialize(inputSplit, taskAttemptContext);
		return dataExtrapolatorRecordReader;
	}

	@Override
	public List<InputSplit> getSplits(JobContext jobContext) throws IOException,
			InterruptedException {
		// get the number of map task configured for
		int numOfSplits = jobContext.getConfiguration().getInt( NUM_MAP_TASKS, -1);
		List<InputSplit> splits = new ArrayList<InputSplit>();
		for(int i=0; i < numOfSplits ; i++){
			splits.add(new DummyInputSplit());
		}
		return splits;
	}
	
	

}

