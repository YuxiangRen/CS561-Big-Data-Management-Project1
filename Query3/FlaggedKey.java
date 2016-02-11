package Query3;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class FlaggedKey implements WritableComparable<FlaggedKey>{
	private Text joinkey = new Text();
	private IntWritable Flag = new IntWritable();
	public static final IntWritable Cust_RECORD = new IntWritable(0);
	public static final IntWritable Trans_RECORD = new IntWritable(1);
	
	public Text getJoinKey(){
		return joinkey;
	}
	public IntWritable getFlag(){
		return Flag;
	}
	
	public FlaggedKey(){}
	
	public FlaggedKey(String key, int order){
		joinkey.set(key);
		Flag.set(order);
	}
	
	@Override
	public int compareTo(FlaggedKey fkey) {
		int compareVal = this.joinkey.compareTo(fkey.getJoinKey());
		if (compareVal == 0) compareVal = this.Flag.compareTo(fkey.getFlag());
		return compareVal;
	}
	@Override
	public void readFields(DataInput In) throws IOException {
		joinkey.readFields(In);
		Flag.readFields(In);
	}
	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		joinkey.write(out);
		Flag.write(out);
	}
	
	public boolean equals (FlaggedKey other) {
	    return this.joinkey.equals(other.getJoinKey()) && this.Flag.equals(other.getFlag() );
	}

	public int hashCode() {
	    return this.joinkey.hashCode();
	}
		
	}
