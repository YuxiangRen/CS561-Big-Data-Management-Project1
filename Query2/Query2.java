package Query2;

import java.io.IOException;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;



public class Query2 {
	
	private static class IntArrayWritable extends ArrayWritable{

		public IntArrayWritable() {
			super(IntWritable.class);
		}
		
		public IntArrayWritable(String[] strings){
			super(IntWritable.class);
			IntWritable[] nums = new IntWritable[strings.length];
			for (int i = 0; i < strings.length; i ++){
				nums[i] = new IntWritable(Integer.parseInt(strings[i]));
			}
			set(nums);
		}
		
	}
	
	public static class Map extends  Mapper<Object, Text, Text, IntArrayWritable>{
		private Text ID = new Text();
		
		public void map(Object key, Text value, Context context)  throws IOException, InterruptedException {
			String[] tokens = value.toString().split(",");
			ID.set(tokens[1]);
			String[] vals = new String[2];
			vals[0] = tokens[3];
			vals[1] = "1";
			context.write(ID, new IntArrayWritable(vals));
		}
		
	}
	
	private static class ArrayCombiner extends Reducer<Text, IntArrayWritable, Text, IntArrayWritable>{
		public void reduce(Text key, Iterable<IntArrayWritable> values, Context context) throws IOException, InterruptedException{
			int transSum = 0;
			int totalSum = 0;
			for (IntArrayWritable i : values){
				Writable[] writable = i.get();
				transSum += ((IntWritable)writable[0]).get();
				totalSum += ((IntWritable)writable[1]).get();
			}
			String[] joint = {Integer.toString(transSum),Integer.toString(totalSum)};
			context.write(key, new IntArrayWritable(joint));
		}
	}
	
	public static class IntSumReducer extends Reducer<Text,IntArrayWritable,Text,Text>{
		private Text result = new Text();
		
		public void reduce(Text key, Iterable<IntArrayWritable> values, Context context) throws IOException, InterruptedException{
			int transSum = 0;
			int totalSum = 0;
			for (IntArrayWritable i : values){
				Writable[] writable = i.get();
				transSum += ((IntWritable)writable[0]).get();
				totalSum += ((IntWritable)writable[1]).get();
			}
			String joint = Integer.toString(transSum) +" "+ Integer.toString(totalSum);
			result.set(joint);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		long start = new Date().getTime();
	    Configuration conf = new Configuration();
	    if (args.length != 2) {
	      System.err.println("Usage: CustomerID Aggregation <HDFS input file> <HDFS output file>");
	      System.exit(2);
	    }
	    Job job = new Job(conf, "CustomerID Agg");
	    job.setJarByClass(Query2.class);
	    job.setMapperClass(Map.class);
	    //job.setCombinerClass(ArrayCombiner.class);
	    job.setReducerClass(IntSumReducer.class);
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(IntArrayWritable.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    job.setNumReduceTasks(4);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    job.waitForCompletion(true);
		long end = new Date().getTime();
		System.out.println("Job took "+(end-start) + "milliseconds"); 

	}

}
