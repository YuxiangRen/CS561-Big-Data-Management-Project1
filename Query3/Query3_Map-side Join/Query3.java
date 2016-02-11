package Project1.Query3;

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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;

public class Query3 {
	
	public static class JoinMapper
	extends Mapper<LongWritable, Text, CustomerInfo, Text>{
		
		private HashMap<Integer, CustomerInfo> customers = new HashMap<Integer, CustomerInfo>();
		
		private void readCustomersFile(String line) throws IOException{
			
		/*	List<String> lines = FileUtils.readLines(new File(path.toUri()));
			for(String line : lines){
				String[] recordFields = line.split(",");
				int customerId = Integer.parseInt(recordFields[0]); //customerID
	            String name = recordFields[1];
	            float salary = Float.parseFloat(recordFields[4]);
	            customers.put(customerId, new CustomerInfo(customerId, name, salary));
			}*/
			
					String[] recordFields = line.split(",");
			        int customerId = Integer.parseInt(recordFields[0]); //customerID
			        String name = recordFields[1];
			        float salary = Float.parseFloat(recordFields[4]);
			        customers.put(customerId, new CustomerInfo(customerId, name, salary));
				
		}
		
		public void map(LongWritable key, Text value, Context context
				)throws IOException, InterruptedException{
			
			String[] recordFields = value.toString().split(",");
			int customerId =  Integer.parseInt(recordFields[1]);
			int numOfTransactions = 1;
			float transTotal = Float.parseFloat(recordFields[2]);
			int numOfItems = Integer.parseInt(recordFields[3]);
			
			CustomerInfo customerInfo = customers.get(customerId);
			
			String outValue = numOfTransactions + "," 
								+ transTotal + ","
								+ numOfItems;
			
			context.write(customerInfo, new Text(outValue));
			
		}
		
		@Override
		public void setup(Context context) throws IOException, InterruptedException {
//			if(context.getJobName()==null||"".equals(context.getJobName())){
//				System.out.println("context is invalid!");
//			}
			Configuration conf = context.getConfiguration();
			URI[] uris = DistributedCache.getCacheFiles(conf);
			
			/*DistributedCache.addLocalFiles(conf, "/home/hadoop/Downloads/input/customers.txt");
			
	        Path[] paths = DistributedCache.getLocalCacheFiles(conf);
	        readCustomersFile(paths[0]);*/
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
	}
	
	private static class JoinReducer
	extends Reducer<CustomerInfo, Text, IntWritable, Text>{
		public void reduce(CustomerInfo key, Iterable<Text> values, Context context
				)throws IOException, InterruptedException{
			
			int numOfTransactions = 0;
			float totalSum = 0;
			int minItems = 10;
			for(Text value: values){
				String[] recordFields = value.toString().split(",");
				numOfTransactions += Integer.parseInt(recordFields[0]);
				totalSum += Float.parseFloat(recordFields[1]);
				int numOfItems = Integer.parseInt(recordFields[2]);
				minItems = numOfItems >= minItems ? minItems:numOfItems;
			}
			
			String outValue = key.toString() + "," 
							  + numOfTransactions + ","
							  + totalSum + ","
							  + minItems;
			
			context.write(null, new Text(outValue));
		}
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception{
		 long start = System.currentTimeMillis();
		 Configuration conf = new Configuration();
		 DistributedCache.addCacheFile(new URI("hdfs://localhost:8020/user/hadoop/input/customers.txt"), conf);
		 Job job = new Job(conf, "query3");
		 job.setJarByClass(Query3.class);
		 
		 job.setMapperClass(JoinMapper.class);
		 job.setMapOutputKeyClass(CustomerInfo.class);
		 
		 job.setReducerClass(JoinReducer.class);
		 job.setNumReduceTasks(2);
		 
		 job.setOutputKeyClass(IntWritable.class);
		 job.setOutputValueClass(Text.class);
		 
		 FileInputFormat.addInputPath(job, new Path("hdfs://localhost:8020/user/hadoop/input/transactions.txt"));
		 FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:8020/user/hadoop/outputQ3/"));
		 
		 job.waitForCompletion(true);
		 long end = System.currentTimeMillis();
		 System.out.println(end-start);

	}

}
