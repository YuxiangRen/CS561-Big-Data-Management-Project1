package Query3;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class TransInfo implements Writable{
	private IntWritable numOfTrans = new IntWritable(1);
	private FloatWritable totalSum = new FloatWritable();
	private IntWritable MinItem = new IntWritable();
	
	public TransInfo(){}
	
	public TransInfo(float sum, int min){
		this.totalSum.set(sum);
		this.MinItem.set(min);
	}
	
	public IntWritable getTrans(){
		return numOfTrans;
	}
	
	public FloatWritable getTotal(){
		return totalSum;
	}
	
	public IntWritable getMin(){
		return MinItem;
	}
	

	@Override
	public void readFields(DataInput In) throws IOException {
		totalSum.readFields(In);
		MinItem.readFields(In);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		totalSum.write(out);
		MinItem.write(out);
	}
	
}
