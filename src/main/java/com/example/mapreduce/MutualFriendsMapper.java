/*
 * (c) Cloud for Beginners.
 * 
 * author: tmusabbir
 */
package com.example.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The Class MutualFriendsMapper.
 */
public class MutualFriendsMapper extends MapReduceBase implements Mapper<LongWritable, Text, FriendPair, Text> {

	/** The Constant LOGGER. */
	private static final Logger LOGGER = LoggerFactory.getLogger(MutualFriendsMapper.class);


	/* (non-Javadoc)
	 * @see org.apache.hadoop.mapred.Mapper#map(java.lang.Object, java.lang.Object, org.apache.hadoop.mapred.OutputCollector, org.apache.hadoop.mapred.Reporter)
	 */
	public void map(LongWritable key, Text value, OutputCollector<FriendPair, Text> output, Reporter reporter)
			throws IOException {
		String[] fullFrndList = value.toString().split(MutualFriends.NAME_LIST_SPLIT);
		String frndName = fullFrndList[0];
		String[] listOfFrinds = fullFrndList[1].split(MutualFriends.LIST_SPLIT);
		for (String frnd : listOfFrinds) {
			FriendPair fp = new FriendPair(new Text(frndName), new Text(frnd));
			LOGGER.debug("Emitting: key: " + fp + ", value: " + fullFrndList[1]);
			output.collect(fp, new Text(fullFrndList[1]));
		}

	}
}
