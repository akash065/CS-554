import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 
 * @author Diego Pino García <dpino@igalia.com>
 * 
 * Canonical implementation at http://wiki.apache.org/hadoop/WordCount
 *
 */
public class WordCount extends Configured implements Tool {
	
	public static class MapClass extends
			Mapper<Object, Text, Text, IntWritable> {

		private static final IntWritable ONE = new IntWritable(1);
		private Text word = new Text();

		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

		 
			Thread.sleep(1);
		}
		}
	}
		
	public static class Reduce extends
			Reducer<Text, IntWritable, Text, IntWritable> {

		private IntWritable count = new IntWritable();

		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {

			
		}
	}

	public int run(String[] arg0) throws Exception {		
        Job job = new Job(getConf());
		job.setJarByClass(WordCount.class);
		job.setJobName("wordcount");
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
        job.setMapperClass(MapClass.class);
        job.setReducerClass(Reduce.class);

		FileInputFormat.setInputPaths(job, new Path(arg0[arg0.length-2]));
		FileOutputFormat.setOutputPath(job, new Path(arg0[arg0.length-1]));

		boolean success = job.waitForCompletion(true);
		return success ? 0 : 1; 
	}
	
	public static void main(String[] args) throws Exception {
		long startTime = System.nanoTime();
		int res = ToolRunner.run(new Configuration(), new WordCount(), args);
		long endTime = System.nanoTime();
		long duration=endTime-startTime;
		System.out.println("Total time to compute the program is " + duration +" ns");
		//System.out.println(duration);
		System.exit(res);
	}
}