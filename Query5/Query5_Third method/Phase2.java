package Project1.Query5;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Phase2 {
	
	public static class MinTransMapper
	extends Mapper<LongWritable, Text, IntWritable, Text>{
		IntWritable outKey = new IntWritable(0);
		IntWritable one = new IntWritable(1);
		public void map(LongWritable key, Text value, Context context
				)throws IOException, InterruptedException{
			context.write(one, value);
		}
	}
	
	private static class MinTransReducer
	extends Reducer<IntWritable, Text, IntWritable, Text>{
		
		private HashMap<Integer, String> customers = new HashMap<Integer, String>();
		
		private void readCustomersFile(String line) throws IOException{
						String[] recordFields = line.split(",");
				        int customerId = Integer.parseInt(recordFields[0]); //customerID
				        String name = recordFields[1].toString();
				        customers.put(customerId, name);
			}
		
		@Override
		public void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			URI[] uris = DistributedCache.getCacheFiles(conf);
			
	        if(uris!=null){
	        	System.out.println(uris[0]);
		        FSDataInputStream dataIn = FileSystem.get(context.getConfiguration()).open(new Path(uris[0]));
		        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(dataIn));
		        
		        String line = bufferedReader.readLine();
		        readCustomersFile(line);
		        while (line!= null&&!"".equals(line)) {
		        	line = bufferedReader.readLine();
		        	if(line!=null){
		        		readCustomersFile(line);
		        	}
		        }
	        }
	        else{
	        	System.out.println("uris are null!");
	        }
	    }
		
		public void reduce(IntWritable key, Iterable<Text> values, Context context
				)throws IOException, InterruptedException{
			
			int minNumOfTrans = 50000;
			int customerId = 0;
			for(Text value : values){
				String[] recordFields = value.toString().split(",");
				int numOfTrans = Integer.parseInt(recordFields[1]);
				if(minNumOfTrans >= numOfTrans){
					customerId = Integer.parseInt(recordFields[0]);
					minNumOfTrans = numOfTrans;
				}
			}
			String name = customers.get(customerId);
			
			String outValue = name + "," + minNumOfTrans;
			
			context.write(null, new Text(outValue));
			
		}
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception{
		 Configuration conf = new Configuration();
		 DistributedCache.addCacheFile(new URI("hdfs://localhost:8020/user/hadoop/input/customers.txt"), conf);
		 Job job = new Job(conf, "Phase2");
		 job.setJarByClass(Phase2.class);
		 
		 job.setMapperClass(MinTransMapper.class);
		 job.setMapOutputKeyClass(IntWritable.class);
		 job.setMapOutputValueClass(Text.class);
		 
		 job.setReducerClass(MinTransReducer.class);
		 job.setNumReduceTasks(1);
		 
		 job.setOutputKeyClass(IntWritable.class);
		 job.setOutputValueClass(Text.class);
		 
		 FileInputFormat.addInputPath(job, new Path("hdfs://localhost:8020/user/hadoop/temp/part-r-00000"));
		 FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:8020/user/hadoop/outputQ5/"));
		 
		 job.waitForCompletion(true);

	}

}
