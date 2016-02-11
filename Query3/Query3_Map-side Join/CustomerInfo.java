package Project1.Query3;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class CustomerInfo<T> implements WritableComparable<T>{
	
	private IntWritable  customerId = new IntWritable();
	private Text name = new Text();
	private FloatWritable salary = new FloatWritable();
	
	public CustomerInfo(){}
	public CustomerInfo(int customerId, String name){
		this.customerId.set(customerId);
		this.name.set(name);
	}
	public CustomerInfo(int customerId, String name, float salary){
		this.customerId.set(customerId);
		this.name.set(name);
		this.salary.set(salary);
	}
	
	public void setCustomerId(int customerId){
		this.customerId.set(customerId);
	}
	public int getCustomerId(){
		return this.customerId.get();
	}
	
	public void setName(String name){
		this.name.set(name);
	}
	public Text getName(){
		return this.name;
	}
	
	public void setSalary(float salary){
		this.salary.set(salary);
	}
	public float getSalary(){
		return this.salary.get();
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.customerId.readFields(in);
		this.name.readFields(in);
		this.salary.readFields(in);
		// TODO Auto-generated method stub
		
	}

	@Override
	public void write(DataOutput out) throws IOException {
		this.customerId.write(out);
		this.name.write(out);
		this.salary.write(out);
	}

	@Override
	public int compareTo(T o) {
		CustomerInfo ci = (CustomerInfo)o;
		return this.customerId.compareTo(ci.customerId);
	}
	
	public String toString(){
		return "" + this.customerId.get() + "," 
				+ this.name.toString() + "," 
				+ this.salary.get();
	}

}
