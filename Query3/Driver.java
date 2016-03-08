package Query3;


import java.io.IOException;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;


public class Driver extends Configured implements Tool{
	
	
	/***
	 * Group output from different relations based on JoinKey
	 * @author Xiang Huang, Zhongzheng Shu
	 *
	 */
	public static class JoinGroupingComparator extends WritableComparator {
	    public JoinGroupingComparator() {
	        super (FlaggedKey.class, true);
	    }                             

	    @Override
	    public int compare (WritableComparable a, WritableComparable b){
	        FlaggedKey first = (FlaggedKey) a;
	        FlaggedKey second = (FlaggedKey) b;
	                      
	        return first.getJoinKey().compareTo(second.getJoinKey());
	    }
	}
	
	/***
	 * Sort records to ensure the first records all come from certain relation
	 * @author Xiang Huang, Zhongzheng Shu
	 *
	 */
	public static class JoinSortingComparator extends WritableComparator {
	    public JoinSortingComparator()
	    {
	    	super(FlaggedKey.class, true);
	    }
	                               
	    @Override
	    public int compare (WritableComparable a, WritableComparable b){
	        FlaggedKey first = (FlaggedKey) a;
	        FlaggedKey second = (FlaggedKey) b;
	                                 
	        return first.compareTo(second);
	    }
	}
	
	//Mappers
	
	public static class CustMapper extends Mapper<Object, Text, FlaggedKey, JoinGenericWritable>{
		
	    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {                           
			String[] tokens = value.toString().split(",");
			String ID = tokens[0];
			String name= tokens[1];
			float salary = Float.parseFloat(tokens[4]);
			
	        FlaggedKey recordKey = new FlaggedKey(ID, FlaggedKey.Cust_RECORD.get());
	        CustInfo record = new CustInfo(name, salary);                                               
	        JoinGenericWritable genericRecord = new JoinGenericWritable(record);
	        context.write(recordKey, genericRecord);
	    }
	}
	          
	public static class TransMapper extends Mapper<Object, Text, FlaggedKey, JoinGenericWritable>{
		
	    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] tokens = value.toString().split(",");
			String ID = tokens[1];
			Float transTotal= Float.parseFloat(tokens[2]);
			int transNum = Integer.parseInt(tokens[3]);
			
	        FlaggedKey recordKey = new FlaggedKey(ID, FlaggedKey.Trans_RECORD.get());
	        TransInfo record = new TransInfo(transTotal, transNum);
	        JoinGenericWritable genericRecord = new JoinGenericWritable(record);
	        context.write(recordKey, genericRecord);
	    }
	}
	
	public static class JoinRecuder extends Reducer<FlaggedKey, JoinGenericWritable, NullWritable, Text>{
	    public void reduce(FlaggedKey key, Iterable<JoinGenericWritable> values, Context context) throws IOException, InterruptedException{
	        StringBuilder output = new StringBuilder();
	        int sumTrans = 0;
	        float totalSum = 0;
	        int minitem = Integer.MAX_VALUE;
	                                               
	        for (JoinGenericWritable v : values) {
	            Writable record = v.get();
	            if (key.getFlag().equals(FlaggedKey.Cust_RECORD)){
	               CustInfo cRecord = (CustInfo)record;
	                output.append(key.getJoinKey().toString()).append(",");
	                output.append(cRecord.getname().toString()).append(",");
	                output.append(cRecord.getsalary().get()+"").append(",");
	            } else {
	                TransInfo trecord = (TransInfo)record;
	                sumTrans += trecord.getTrans().get();
	                totalSum += trecord.getTotal().get();
	                int m1 = trecord.getMin().get();
	                if (m1 < minitem) minitem = m1;
	            }
	        }
	            context.write(NullWritable.get(), new Text(output.toString() + sumTrans + "," + totalSum + ","+ minitem));
	    }
	}
	
	
	public static void main(String[] args) throws Exception{
		// TODO Auto-generated method stub
		long start = new Date().getTime();
	    Configuration conf = new Configuration();
	    int res = ToolRunner.run(new Driver(), args);
		long end = new Date().getTime();
		System.out.println("Job took "+(end-start) + "milliseconds");  
	}

	@Override
	public int run(String[] allArgs) throws Exception {
	    String[] args = new GenericOptionsParser(getConf(), allArgs).getRemainingArgs();
        
	    Job job = Job.getInstance(getConf());
	    job.setJarByClass(Driver.class);
	                               
	    job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
	                               
	    job.setMapOutputKeyClass(FlaggedKey.class);
	    job.setMapOutputValueClass(JoinGenericWritable.class);
	                               
	    MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, CustMapper.class);
	    MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, TransMapper.class);
	                              
	    job.setReducerClass(JoinRecuder.class);
	                         
	    job.setSortComparatorClass(JoinSortingComparator.class);
	    job.setGroupingComparatorClass(JoinGroupingComparator.class);
	    job.setNumReduceTasks(4);                           
	    job.setOutputKeyClass(NullWritable.class);
	    job.setOutputValueClass(Text.class);
	                               
	    FileOutputFormat.setOutputPath(job, new Path(allArgs[2]));
	    boolean status = job.waitForCompletion(true);
	    if (status) {
	        return 0;
	    } else {
	        return 1;
	    }    
	}

}
