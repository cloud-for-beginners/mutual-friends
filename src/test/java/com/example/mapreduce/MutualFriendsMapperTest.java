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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mrunit.MapDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Log4jConfigurer;


/**
 * The Class MutualFriendsMapperTest.
 */
public class MutualFriendsMapperTest {

	/** The Constant LOGGER. */
	private static final Logger LOGGER = LoggerFactory.getLogger(MutualFriendsMapperTest.class);

	/** The test mapper. */
	private Mapper<LongWritable, Text, FriendPair, Text> testMapper;

	/** The test driver. */
	private MapDriver<LongWritable, Text, FriendPair, Text> testDriver;

	/** The Constant INPUT_STR. */
	public static final String INPUT_STR = "Mark->John,Miller,Peter,Robin";


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
		testDriver = MapDriver.newMapDriver(testMapper);
	}


	/**
	 * Test mapper.
	 * 
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	@Test
	public void testMapper() throws IOException {
		List<Pair<FriendPair, Text>> results = testDriver.withInput(new LongWritable(1), new Text(INPUT_STR)).run();
		boolean found = false;

		assertTrue(results.size() == 4);
		for (Pair<FriendPair, Text> p : results) {
			if (p.getFirst().getFirstFriend().toString().equalsIgnoreCase("Mark")
					&& p.getFirst().getSecondFriend().toString().equalsIgnoreCase("Peter")) {
				if (p.getSecond().toString().equals("John,Miller,Peter,Robin")) {
					found = true;
				}
			}
			LOGGER.info("Key: " + p.getFirst() + ", Value: " + p.getSecond());
		}
		assertTrue(found);
	}
}
