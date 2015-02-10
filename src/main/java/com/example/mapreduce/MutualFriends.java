/*
 * (c) Cloud for Beginners.
 * 
 * author: tmusabbir
 */
package com.example.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Log4jConfigurer;


/**
 * The Class MutualFriends.
 */
public class MutualFriends extends Configured implements Tool {

	/** The Constant LOGGER. */
	private static final Logger LOGGER = LoggerFactory.getLogger(MutualFriends.class);

	/** The Constant NAME_LIST_SPLIT. */
	public static final String NAME_LIST_SPLIT = "->";

	/** The Constant LIST_SPLIT. */
	public static final String LIST_SPLIT = ",";


	/**
	 * The main method.
	 * 
	 * @param args the arguments
	 * @throws Exception the exception
	 */
	public static void main(String[] args) throws Exception {
		Log4jConfigurer.initLogging("classpath:META-INF/log4j.properties");
		MutualFriends mf = new MutualFriends();
		if ((args == null) || (args != null && args.length != 3)) {
			LOGGER.error("Usage: mutual-friends-0.0.1.jar [generic options] <input> <output>");
			ToolRunner.printGenericCommandUsage(System.err);
		} else {
			int exitCode = ToolRunner.run(mf, args);
			System.exit(exitCode);
		}

	}


	/* (non-Javadoc)
	 * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
	 */
	public int run(String[] args) throws Exception {
		int ret = 0;
		LOGGER.info("Starting mutual friends map-reduce job...");

		Path inputPath = new Path(args[0]);
		Path outputPath = new Path(args[1]);

		Configuration hadoopConf = getConf();
		JobConf job = new JobConf(hadoopConf, MutualFriends.class);
		job.setJarByClass(getClass());
		job.setJobName("Mutual Friends");

		FileInputFormat.setInputPaths(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);

		job.setInputFormat(TextInputFormat.class);
		job.setOutputFormat(TextOutputFormat.class);

		job.setMapperClass(MutualFriendsMapper.class);
		job.setMapOutputKeyClass(FriendPair.class);
		job.setMapOutputValueClass(Text.class);

		job.setReducerClass(MutualFriendsReducer.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		JobClient.runJob(job);
		return ret;
	}

}
