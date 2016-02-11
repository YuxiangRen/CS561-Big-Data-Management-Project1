package Query5;



public class Inverse implements Comparable<Inverse>{
	private int count;
	private String name;
	
	Inverse(){}
	
	Inverse(String name, int count){
		this.count = count;
		this.name = name;
	}
	
	int getCount(){
		return this.count;
	}
	
	String getName(){
		return this.name;
	}
	
	void setCount(int i){
		this.count = i;
	}
	
	void addCount(int add){
		this.count += add;
	}

	@Override
	public int compareTo(Inverse o) {
		// TODO Auto-generated method stub
		return this.count >= o.getCount()? 1 : -1;
	}
	
	
}