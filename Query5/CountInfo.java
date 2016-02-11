package Query5;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;


public class CountInfo implements Writable, WritableComparable<CountInfo>{
	private Text name = new Text();
	private IntWritable TransCount = new IntWritable(1);
	
	public CountInfo(){}
	
	public CountInfo(String name, int count){
		this.name.set(name);
		this.TransCount.set(count);
	}
	
	public IntWritable getCount(){
		return TransCount;
	}
	
	public Text getName(){
		return name;
	}
	

	@Override
	public void readFields(DataInput In) throws IOException {
		name.readFields(In);
		TransCount.readFields(In);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		name.write(out);
		TransCount.write(out);
	}

	@Override
	public int compareTo(CountInfo c) {
		// TODO Auto-generated method stub
		return this.TransCount.compareTo(c.getCount());
	}
	
}
