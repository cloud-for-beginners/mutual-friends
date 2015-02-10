/*
 * (c) Cloud for Beginners.
 * 
 * author: tmusabbir
 */
package com.example.mapreduce;

import static org.junit.Assert.assertTrue;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mrunit.MapReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Log4jConfigurer;


/**
 * The Class MutualFriendsReducerTest.
 */
public class MutualFriendsReducerTest {

	/** The Constant LOGGER. */
	private static final Logger LOGGER = LoggerFactory.getLogger(MutualFriendsReducerTest.class);

	/** The test mapper. */
	private Mapper<LongWritable, Text, FriendPair, Text> testMapper;

	/** The test reducer. */
	private Reducer<FriendPair, Text, NullWritable, Text> testReducer;

	/** The test driver. */
	private MapReduceDriver<LongWritable, Text, FriendPair, Text, NullWritable, Text> testDriver;


	/**
	 * Setup.
	 */
	@Before
	public void setup() {
		try {
			Log4jConfigurer.initLogging("classpath:META-INF/log4j.properties");
		} catch (FileNotFoundException e) {
			LOGGER.error("Unable to read log4j.properties file.");
		}
		testMapper = new MutualFriendsMapper();
		testReducer = new MutualFriendsReducer();
		testDriver = MapReduceDriver.newMapReduceDriver(testMapper, testReducer);
	}


	/**
	 * Test reducer.
	 *
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	@Test
	public void testReducer() throws IOException {
		String input1 = "Mark->John,Miller,Peter,Robin";
		String input2 = "Robin->John,Mark,Miller,Russell";
		String input3 = "Russell->Mark,Peter,Robin";

		Pair<LongWritable, Text> pair1 = new Pair<LongWritable, Text>(new LongWritable(1), new Text(input1));
		Pair<LongWritable, Text> pair2 = new Pair<LongWritable, Text>(new LongWritable(1), new Text(input2));
		Pair<LongWritable, Text> pair3 = new Pair<LongWritable, Text>(new LongWritable(1), new Text(input3));

		testDriver.addInput(pair1);
		testDriver.addInput(pair2);
		testDriver.addInput(pair3);
		List<Pair<NullWritable, Text>> results = testDriver.run();

		assertTrue(results.size() == 2);
		boolean found = false;
		for (Pair<NullWritable, Text> p : results) {
			if (p.getSecond().toString().equals("(Mark Robin)-> John, Miller")) {
				found = true;
			}
			LOGGER.debug(p.getSecond().toString());
		}
		assertTrue(found);
	}
}
