package Project1.Query5;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Query5 {
	
	public static class GroupMapper
	extends Mapper<LongWritable, Text, IntWritable, IntWritable>{
		IntWritable outKey = new IntWritable(0);
		IntWritable one = new IntWritable(1);
		public void map(LongWritable key, Text value, Context context
				)throws IOException, InterruptedException{
			
			String[] recordFields = value.toString().split(",");
			int customerId =  Integer.parseInt(recordFields[1]);
			outKey.set(customerId);
			
			context.write(outKey, one);
		}
	}
	
	private static class GroupReducer
	extends Reducer<IntWritable, IntWritable, IntWritable, Text>{
		public void reduce(IntWritable key, Iterable<IntWritable> values, Context context
				)throws IOException, InterruptedException{
			
			int numOfTransactions = 0;
			for(IntWritable value : values)
			{
				numOfTransactions += value.get();
			}
			
			String outValue = "" + key.get() + ","
								+ numOfTransactions;
			
			context.write(null, new Text(outValue));
			
		}
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception{
		 long start = System.currentTimeMillis();
		 Configuration conf = new Configuration();
		 Job job = new Job(conf, "query5");
		 job.setJarByClass(Query5.class);
		 
		 job.setMapperClass(GroupMapper.class);
		 job.setMapOutputKeyClass(IntWritable.class);
		 job.setMapOutputValueClass(IntWritable.class);
		 
//		 job.setGroupingComparatorClass(cls)
		 
		 job.setReducerClass(GroupReducer.class);
		 job.setNumReduceTasks(1);
		 
		 job.setOutputKeyClass(IntWritable.class);
		 job.setOutputValueClass(Text.class);
		 
		 
		 FileInputFormat.addInputPath(job, new Path("hdfs://localhost:8020/user/hadoop/input/transactions.txt"));
		 FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:8020/user/hadoop/temp/"));
		 
		 job.waitForCompletion(true);
		 
		 Phase2.main(args);
		 
		 long end = System.currentTimeMillis();
		 
		 System.out.println((end-start)/1000.0);

	}

}
