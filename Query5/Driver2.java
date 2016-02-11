package Query5;



import java.util.*;
import java.io.*;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.commons.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;

import Query5.Driver1.CountReducer;
import Query5.Driver1.NameMapper;

import java.net.URI;

public class Driver2 {
	private static final String OUTPUT_PATH = "intermediate_output";
	
public static class NameMapper extends Mapper<Object, Text, Text, IntWritable>{
		
		private HashMap<Integer, String> ID_Name_Map = new HashMap<Integer,String>();
		private static final IntWritable num = new IntWritable(1);

	    protected void setup(Context context) throws IOException{
	    	String[] tokens;
	    	Configuration conf = context.getConfiguration();
	    	URI[] uris = DistributedCache.getCacheFiles(conf);
	    	FSDataInputStream dataIn = FileSystem.get(context.getConfiguration()).open(new Path(uris[0]));
	    	BufferedReader br = new BufferedReader(new InputStreamReader(dataIn));
	    	String line = br.readLine();
	    	while (line != null){
	    		tokens = line.split(",");
	            int key = Integer.parseInt(tokens[0]);
	            String name = tokens[1];
	            ID_Name_Map.put(key, name);
	    		line = br.readLine();
	    	}
	    }
		
	    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {                           
			String[] tokens = value.toString().split(",");
			Text Name = new Text();
			int joinID = Integer.parseInt(tokens[1]);
			Name.set(ID_Name_Map.get(joinID));
	        context.write(Name, num);
	    }
	}

	public static class CountReducer extends Reducer<Text, IntWritable, NullWritable, Text>{
		
		
	    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{   
	    	int counter = 0;
	    	for (IntWritable v: values){
	    		counter += v.get();
	    	}
	    	context.write(NullWritable.get(), new Text(key.toString() +","+ counter));
	    }
	}
	public static class LocMapper extends Mapper<Object, Text, IntWritable, CountInfo>{
		private static final IntWritable loc = new IntWritable(1);
		private int min = Integer.MAX_VALUE;
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] tokens = value.toString().split(",");
			String name = tokens[0];
			int count = Integer.parseInt(tokens[1]);
			if (count <= min){
				min = count;
				CountInfo ci = new CountInfo(name, count);
		        context.write(loc, ci);
			}
	    }
	}
	
	
	public static class MinReducer extends Reducer<IntWritable, CountInfo, NullWritable, Text>{
		private static List<String> nameList = new ArrayList<String>();
		private static int min = Integer.MAX_VALUE;
		
		public void reduce(IntWritable key, Iterable<CountInfo> values, Context context) throws IOException, InterruptedException{
			for (CountInfo c : values){
				int tmpnum = c.getCount().get();
				if (tmpnum < min){
					min = tmpnum;
					nameList = new ArrayList<String>();
					nameList.add(c.getName().toString());
				}else if (tmpnum == min){
					nameList.add(c.getName().toString());
				}
			}
			for (String name : nameList){
			context.write(NullWritable.get(), new Text(name +","+min));
			}
		}
	}
	
	public static void main(String[] args) throws Exception{
		// TODO Auto-generated method stub
		long start = new Date().getTime();
	    Configuration conf = new Configuration();
	    conf.set("mapred.jop.tracker", "hdfs://localhost:8020");
	    conf.set("fs.default.name", "hdfs://localhost:8020");
	    DistributedCache.addCacheFile(new URI("hdfs://localhost:8020/user/hadoop/input/customers.txt"), conf);
	    Job job = new Job(conf, "Name Count");
	    job.setJarByClass(Driver2.class);
	    
	    job.setMapperClass(NameMapper.class);
	    job.setReducerClass(CountReducer.class);  
	    
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(IntWritable.class); 
	    job.setOutputKeyClass(NullWritable.class);
	    job.setOutputValueClass(Text.class);
	    job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
	    job.setNumReduceTasks(4);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));
	    job.waitForCompletion(true);
	    
	    Job job2 = new Job(conf, "Min Count");
	    job2.setJarByClass(Driver2.class);
	    
	    job2.setMapperClass(LocMapper.class);
	    //job2.setSortComparatorClass(SortingComparator.class);
	    job2.setReducerClass(MinReducer.class);
	    
	    job2.setMapOutputKeyClass(IntWritable.class);
	    job2.setMapOutputValueClass(CountInfo.class); 
	    job2.setOutputKeyClass(NullWritable.class);
	    job2.setOutputValueClass(Text.class);
	    
	    job2.setInputFormatClass(TextInputFormat.class);
	    job2.setOutputFormatClass(TextOutputFormat.class);
	    
	    TextInputFormat.addInputPath(job2, new Path(OUTPUT_PATH));
	    TextOutputFormat.setOutputPath(job2, new Path(args[1]));

	   job2.waitForCompletion(true);
	   FileSystem fs = FileSystem.get(conf);
	   fs.delete(new Path(OUTPUT_PATH), true);
	    
		long end = new Date().getTime();
		System.out.println("Job took "+(end-start) + "milliseconds");    	    
	}
	
}
