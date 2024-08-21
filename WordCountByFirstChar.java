package au.edu.rmit.cosc2637.s3766477.task2;

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
 * This class consists of MapReduce program to count the number of words that
 * begin with a vowel and count of all how many words that begin with a
 * consonant.
 * 
 * @author Sarthak Sirari (S3766477)
 *
 */
public class WordCountByFirstChar {

	// Declare constant values
	private static final String JOB_NAME = "Word Count by First Char";

	/**
	 * This class acts as the Mapper class for the MapReduce program of
	 * WordCountByFirstChar by extending Mapper class.
	 * 
	 * @author Sarthak Sirari (S3766477)
	 *
	 */
	public static class WordCountByFirstCharMapper extends Mapper<Object, Text, Text, IntWritable> {

		// Declare constant values
		private static final String LOG_MAPPER_INFO = "The mapper task of Sarthak Sirari, s3766477";
		private static final Logger LOG_MAPPER = Logger.getLogger(WordCountByFirstCharMapper.class);

		private static final IntWritable ONE = new IntWritable(1);

		private static final Text VOWEL_WORDS = new Text("Vowel Words");
		private static final Text CONSONANT_WORDS = new Text("Consonant Words");

		/**
		 * Override the map method of Mapper class to define the Mapper functionality.
		 */
		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			// Log mapper information
			LOG_MAPPER.info(LOG_MAPPER_INFO);

			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {

				// Check if the first character is a vowel or a consonant
				char firstChar = itr.nextToken().trim().toLowerCase().charAt(0);
				if (firstChar == 'a' || firstChar == 'e' || firstChar == 'i' || firstChar == 'o' || firstChar == 'u') {
					context.write(VOWEL_WORDS, ONE);
				} else if (firstChar >= 'a' && firstChar <= 'z') {
					context.write(CONSONANT_WORDS, ONE);
				}
			}
		}
	}

	/**
	 * This class acts as the Reducer class for the MapReduce program of
	 * WordCountByFirstChar by extending Reducer class.
	 * 
	 * @author Sarthak Sirari (S3766477)
	 *
	 */
	public static class WordCountByFirstCharReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

		// Declare constant values
		private static final String LOG_REDUCER_INFO = "The reducer task of Sarthak Sirari, s3766477";
		private static final Logger LOG_REDUCER = Logger.getLogger(WordCountByFirstCharReducer.class);

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
		job.setJarByClass(WordCountByFirstChar.class);

		// Set the Mapper class
		job.setMapperClass(WordCountByFirstCharMapper.class);

		// Set the Combiner class
		job.setCombinerClass(WordCountByFirstCharReducer.class);

		// Set the Reducer class
		job.setReducerClass(WordCountByFirstCharReducer.class);

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
