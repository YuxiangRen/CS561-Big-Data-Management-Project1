package Query1;


import java.io.IOException;
import java.util.Date;
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
import org.apache.hadoop.util.GenericOptionsParser;

import Example.WordCount;
import Example.WordCount.IntSumReducer;
import Example.WordCount.TokenizerMapper;

public class Query1 {
	
	public static class FilterMapper extends Mapper<Object, Text, Text, Text>{
		private Text ID = new Text();
		private Text name = new Text();
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] tokens = value.toString().split(",");
			int id = Integer.parseInt(tokens[3]);
			if (id >=2 && id <=6){
				ID.set(tokens[3]);
				name.set(tokens[1]);
				context.write(ID, name);
			}
		}
	}

	public static void main(String[] args) throws Exception{
		// TODO Auto-generated method stub
		long start = new Date().getTime();
		 Configuration conf = new Configuration();
		 if (args.length != 2) {
		      System.err.println("Usage: ID Filter<HDFS input file> <HDFS output file>");
		      System.exit(2);
		 }
		 Job job = new Job(conf, "Filter");
		 job.setJarByClass(Query1.class);
		 job.setMapperClass(FilterMapper.class);
		 job.setOutputKeyClass(Text.class);
		 job.setNumReduceTasks(0);
		 job.setOutputValueClass(Text.class);
		 FileInputFormat.addInputPath(job, new Path(args[0]));
		 FileOutputFormat.setOutputPath(job, new Path(args[1]));
		 boolean status = job.waitForCompletion(true);
		long end = new Date().getTime();
		System.out.println("Job took "+(end-start) + "milliseconds"); 

	}

}
