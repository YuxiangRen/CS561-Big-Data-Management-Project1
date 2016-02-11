package Query3;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class CustInfo implements Writable{
	private Text name = new Text();
	private FloatWritable salary = new FloatWritable();
	
	public CustInfo(){}
	
	public Text getname(){
		return name;
	}
	
	public FloatWritable getsalary(){
		return salary;
	}
	
	
	public CustInfo(String name, float salary){
		super();
		this.name.set(name);
		this.salary.set(salary);
	}

	@Override
	public void readFields(DataInput In) throws IOException {
		name.readFields(In);
		salary.readFields(In);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		name.write(out);
		salary.write(out);
	}
	
}
