# teragen
This code generates terabytes of data using Hadoop MapReduce

For executing data extrapolation jar user needs to fire command in below format
bin/hadoop jar dataextrapolator.jar com.impetus.datagenerator.DataExtrapolatorDriver 
  <PathToFileWhichDataWeWantToExtrapolate zipdata.csv>   <PathToFileWhichDataWeWantToExtrapolate channeldata.csv> 1 
	  <Output Directory>
