package au.edu.rmit.cosc2637.s3766477.task3;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

/**
 * This class consists of MapReduce program to count the number of each word
 * using the in-mapper combining, rather than an independent combiner.
 * 
 * @author Sarthak Sirari (S3766477)
 *
 */
public class WordCountWithInMapperCombining {

	// Declare constant values
	private static final String JOB_NAME = "Word Count With In-Mapper Combining";

	/**
	 * This class acts as the Mapper class for the MapReduce program of
	 * WordCountWithInMapperCombining by extending Mapper class.
	 * 
	 * @author Sarthak Sirari (S3766477)
	 *
	 */
	public static class WordCountWithInMapperCombiningMapper extends Mapper<Object, Text, Text, IntWritable> {

		// Declare constant values
		private static final String LOG_MAPPER_INFO = "The mapper task of Sarthak Sirari, s3766477";
		private static final Logger LOG_MAPPER = Logger.getLogger(WordCountWithInMapperCombiningMapper.class);

		// Declare a HashMap to store the count to each word
		private Map<String, Integer> wordCount = new HashMap<String, Integer>();

		/**
		 * Override the map method of Mapper class to define the Mapper functionality.
		 */
		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			// Log mapper information
			LOG_MAPPER.info(LOG_MAPPER_INFO);

			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {

				// Check if the word has already encountered before and increase its count in
				// the HashMap, else put the new word in the HashMap to implement in-mapper
				// combining functionality
				String word = itr.nextToken();
				if (wordCount.containsKey(word)) {
					wordCount.put(word, (int) wordCount.get(word) + 1);
				} else {
					wordCount.put(word, 1);
				}
			}
		}

		/**
		 * Override the cleanup method of Mapper class to write all the values from the
		 * HashMap to the context after scanning all the input files
		 */
		@Override
		public void cleanup(Context context) throws IOException, InterruptedException {
			Iterator<Map.Entry<String, Integer>> wordCountEntries = wordCount.entrySet().iterator();

			// Write all the entries in the HashMap to the Context
			while (wordCountEntries.hasNext()) {
				Map.Entry<String, Integer> entry = wordCountEntries.next();
				context.write(new Text(entry.getKey()), new IntWritable(entry.getValue()));
			}
		}
	}

	/**
	 * This class acts as the Reducer class for the MapReduce program of
	 * WordCountWithInMapperCombining by extending Reducer class.
	 * 
	 * @author Sarthak Sirari (S3766477)
	 *
	 */
	public static class WordCountWithInMapperCombiningReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

		// Declare constant values
		private static final String LOG_REDUCER_INFO = "The reducer task of Sarthak Sirari, s3766477";
		private static final Logger LOG_REDUCER = Logger.getLogger(WordCountWithInMapperCombiningReducer.class);

		/**
		 * Override the reduce method of Reducer class to define the Reducer
		 * functionality.
		 */
		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {

			// Log reducer information
			LOG_REDUCER.info(LOG_REDUCER_INFO);

			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}

	/**
	 * Main method for running the MapReduce program.
	 * 
	 * @param args Contains the command line arguments, i.e. input and output
	 *             file(s) paths in our case
	 * @throws Exception Throws an exception when an error is occurred when running
	 *                   the program
	 */
	public static void main(String[] args) throws Exception {

		// Declare new configuration object
		Configuration conf = new Configuration();

		// Declare new Job object with the configuration and its name
		Job job = Job.getInstance(conf, JOB_NAME);

		// Set the class name for running the program
		job.setJarByClass(WordCountWithInMapperCombining.class);

		// Set the Mapper class
		job.setMapperClass(WordCountWithInMapperCombiningMapper.class);

		// Set the Combiner class
		job.setCombinerClass(WordCountWithInMapperCombiningReducer.class);

		// Set the Reducer class
		job.setReducerClass(WordCountWithInMapperCombiningReducer.class);

		// Set the Output class Key and Value classes
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		// Set the input and output file paths
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		// Exit the program when the MapReduce job is complete
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
