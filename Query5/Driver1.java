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
import java.util.Map.Entry;
import java.net.URI;

public class Driver1 {
	private static final String OUTPUT_PATH = "intermediate_output";
	
public static class NameMapper extends Mapper<Object, Text, IntWritable, Text>{
		
		private HashMap<Integer, String> ID_Name_Map = new HashMap<Integer,String>();
		private static final IntWritable loc = new IntWritable(1);

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
	        context.write(loc, Name);
	    }
	}


	public static class CountReducer extends Reducer<IntWritable, Text, Text, IntWritable>{
		
		private static HashMap<String, Inverse> Trans_Count = new HashMap<String, Inverse>();
		
	    public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException{                                   
	        for (Text t : values) {
	        	String name =t.toString();
	        	if (!Trans_Count.containsKey(name)){
	        		Trans_Count.put(name, new Inverse(name, 1));
	        	}
	        	else{
	        		Trans_Count.get(name).addCount(1);
	        	}
	        }
	        PriorityQueue<Inverse> q = new PriorityQueue<Inverse>(Trans_Count.values());
	        int min = q.peek().getCount();
	        Inverse tmp;
	        while ((tmp = q.poll()) != null && tmp.getCount() == min){
	        	context.write(new Text(tmp.getName()), new IntWritable (min));
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
	    job.setJarByClass(Driver1.class);
	    
	    job.setMapOutputKeyClass(IntWritable.class);
	    job.setMapOutputValueClass(Text.class); 
	    job.setMapperClass(NameMapper.class);
	    job.setReducerClass(CountReducer.class);                        
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    job.waitForCompletion(true);
		long end = new Date().getTime();
		System.out.println("Job took "+(end-start) + "milliseconds");    	    
	}

}
