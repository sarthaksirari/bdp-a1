package au.edu.rmit.cosc2637.s3766477.task1;

import java.io.IOException;
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
 * This class consists of MapReduce program to count the number of short words
 * (1-4 letters), medium words (5-7 letters) words, long words (8-10 letters),
 * and extra-long words (More than 10 letters).
 * 
 * @author Sarthak Sirari (S3766477)
 *
 */
public class WordCountByLength {

	// Declare constant values
	private static final String JOB_NAME = "Word Count by Length";

	/**
	 * This class acts as the Mapper class for the MapReduce program of
	 * WordCountByLength by extending Mapper class.
	 * 
	 * @author Sarthak Sirari (S3766477)
	 *
	 */
	public static class WordCountByLengthMapper extends Mapper<Object, Text, Text, IntWritable> {

		// Declare constant values
		private static final String LOG_MAPPER_INFO = "The mapper task of Sarthak Sirari, s3766477";
		private static final Logger LOG_MAPPER = Logger.getLogger(WordCountByLengthMapper.class);

		private static final IntWritable ONE = new IntWritable(1);

		private static final Text SHORT_WORDS = new Text("Short Words");
		private static final Text MEDIUM_WORDS = new Text("Medium Words");
		private static final Text LONG_WORDS = new Text("Long Words");
		private static final Text EXTRA_LARGE_WORDS = new Text("Extra Long Words");

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
	 * This class acts as the Reducer class for the MapReduce program of
	 * WordCountByLength by extending Reducer class.
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
		job.setJarByClass(WordCountByLength.class);

		// Set the Mapper class
		job.setMapperClass(WordCountByLengthMapper.class);

		// Set the Combiner class
		job.setCombinerClass(WordCountByLengthReducer.class);

		// Set the Reducer class
		job.setReducerClass(WordCountByLengthReducer.class);

		// Set the Number of output files to 1
		job.setNumReduceTasks(1);

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
