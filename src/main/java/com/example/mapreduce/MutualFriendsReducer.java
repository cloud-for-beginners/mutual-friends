/*
 * (c) Cloud for Beginners.
 * 
 * author: tmusabbir
 */
package com.example.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The Class MutualFriendsReducer.
 */
public class MutualFriendsReducer extends MapReduceBase implements Reducer<FriendPair, Text, NullWritable, Text> {

	/** The Constant LOGGER. */
	private static final Logger LOGGER = LoggerFactory.getLogger(MutualFriendsReducer.class);


	/* (non-Javadoc)
	 * @see org.apache.hadoop.mapred.Reducer#reduce(java.lang.Object, java.util.Iterator, org.apache.hadoop.mapred.OutputCollector, org.apache.hadoop.mapred.Reporter)
	 */
	public void reduce(FriendPair key, Iterator<Text> values, OutputCollector<NullWritable, Text> output,
			Reporter reporter) throws IOException {
		LOGGER.debug("Key: " + key.toString());

		HashSet<String> rec = new HashSet<String>();
		List<String> mutualFrnds = new ArrayList<String>();

		String frnds = values.next().toString();
		String[] names = frnds.split(MutualFriends.LIST_SPLIT);
		for (String name : names) {
			rec.add(name);
		}

		if (values.hasNext()) {
			frnds = values.next().toString();
			names = frnds.split(MutualFriends.LIST_SPLIT);
			for (String name : names) {
				if (rec.contains(name)) {
					mutualFrnds.add(name);
				}
			}
		}

		if (mutualFrnds.size() > 0) {
			String val = key.toString() + MutualFriends.NAME_LIST_SPLIT + " ";
			boolean commaflg = false;
			for (String mutualFrnd : mutualFrnds) {
				if (commaflg) {
					val += ", ";
				}
				val += mutualFrnd;
				commaflg = true;
			}
			output.collect(NullWritable.get(), new Text(val));
		}
	}
}
