package au.edu.rmit.cosc2637.s3766477.task4;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

/**
 * This class consists of MapReduce program to count the number of short words
 * (1-4 letters), medium words (5-7 letters) words, long words (8-10 letters),
 * and extra-long words (More than 10 letters). Plus, implement a partitioner to
 * process short words and extra-long words in one reducer and process medium
 * words and long words in another reducer.
 * 
 * @author Sarthak Sirari (S3766477)
 *
 */
public class WordCountWithPartitioner {

	// Declare constant values
	private static final String JOB_NAME = "Word Count By Length With Partitioner";

	private static final Text SHORT_WORDS = new Text("Short Words");
	private static final Text MEDIUM_WORDS = new Text("Medium Words");
	private static final Text LONG_WORDS = new Text("Long Words");
	private static final Text EXTRA_LARGE_WORDS = new Text("Extra Long Words");

	/**
	 * This class acts as the Mapper class for the MapReduce program of
	 * WordCountWithPartitioner by extending Mapper class.
	 * 
	 * @author Sarthak Sirari (S3766477)
	 *
	 */
	public static class WordCountByLengthMapper extends Mapper<Object, Text, Text, IntWritable> {

		// Declare constant values
		private static final String LOG_MAPPER_INFO = "The mapper task of Sarthak Sirari, s3766477";
		private static final Logger LOG_MAPPER = Logger.getLogger(WordCountByLengthMapper.class);

		private static final IntWritable ONE = new IntWritable(1);

		/**
		 * Override the map method of Mapper class to define the Mapper functionality.
		 */
		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			// Log mapper information
			LOG_MAPPER.info(LOG_MAPPER_INFO);

			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {

				// Check the length of the words for categorization
				String wordString = itr.nextToken().trim();
				if (wordString.length() >= 1 && wordString.length() <= 4) {
					context.write(SHORT_WORDS, ONE);
				} else if (wordString.length() >= 5 && wordString.length() <= 7) {
					context.write(MEDIUM_WORDS, ONE);
				} else if (wordString.length() >= 8 && wordString.length() <= 10) {
					context.write(LONG_WORDS, ONE);
				} else if (wordString.length() > 10) {
					context.write(EXTRA_LARGE_WORDS, ONE);
				}
			}
		}
	}

	/**
	 * This class acts as the Partitioner class for the MapReduce program of
	 * WordCountWithPartitioner by extending Partitioner class.
	 * 
	 * @author Sarthak Sirari (S3766477)
	 *
	 */
	public static class WordCountByLengthPartitioner extends Partitioner<Text, IntWritable> {

		// Declare constant values
		private static final String LOG_PARTITIONER_INFO = "The partitioner task of Sarthak Sirari, s3766477";
		private static final Logger LOG_PARTITIONER = Logger.getLogger(WordCountByLengthPartitioner.class);

		/**
		 * Override the getPartition method of Partitioner class to define the
		 * Partitioner functionality.
		 */
		@Override
		public int getPartition(Text key, IntWritable value, int numReduceTasks) {

			// Log partitioner information
			LOG_PARTITIONER.info(LOG_PARTITIONER_INFO);

			// Check if the word is short or extra large to direct them to one Reducer for
			// processing and direct medium and long words to another Reducer
			if (key.equals(SHORT_WORDS) || key.equals(EXTRA_LARGE_WORDS)) {
				return 0;
			} else {
				return 1 % numReduceTasks;
			}
		}
	}

	/**
	 * This class acts as the Reducer class for the MapReduce program of
	 * WordCountWithPartitioner by extending Reducer class.
	 * 
	 * @author Sarthak Sirari (S3766477)
	 *
	 */
	public static class WordCountByLengthReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

		// Declare constant values
		private static final String LOG_REDUCER_INFO = "The reducer task of Sarthak Sirari, s3766477";
		private static final Logger LOG_REDUCER = Logger.getLogger(WordCountByLengthReducer.class);

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
		job.setJarByClass(WordCountWithPartitioner.class);

		// Set the Mapper class
		job.setMapperClass(WordCountByLengthMapper.class);

		// Set the Combiner class
		job.setCombinerClass(WordCountByLengthReducer.class);

		// Set the Partitioner class
		job.setPartitionerClass(WordCountByLengthPartitioner.class);

		// Set the Reducer class
		job.setReducerClass(WordCountByLengthReducer.class);

		// Set the Number of output files to 2
		job.setNumReduceTasks(2);

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
