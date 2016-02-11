package DBproject1;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;

public class DataGenerateMain {

	public static void main(String[] args) {
		GenerateData gd = new GenerateData();
		try {
			PrintWriter writer = new PrintWriter("customers.txt", "UTF-8");
			for(int i = 0; i < 50000; i ++){
				int ID = i + 1;
				String name = gd.generateName(10, 20);
				int age = gd.generateInteger(10, 70);
				int countryCode = gd.generateInteger(1, 10);
				float salary = gd.generateFloat(100, 10000);
				String customerInfo = ""+ID +","
							+ name + "," 
							+ "" + age + "," 
							+ "" + countryCode + ","
							+ "" + salary;
				writer.println(customerInfo);
			}
			writer.close();
			
			PrintWriter writer2 = new PrintWriter("transactions.txt", "UTF-8");
			for(int i = 0; i < 5000000; i ++){
				int TransID = i + 1;
				int CustID = gd.generateInteger(1, 50000);
				float TransTotal = gd.generateFloat(10, 1000);
				int TransNumItems = gd.generateInteger(1, 10);
				String Description = gd.generateCharacters(20, 50);
				String transactionInfo = ""+TransID +","
							+ ""+CustID + "," 
							+ "" + TransTotal + "," 
							+ "" + TransNumItems + ","
							+ Description;
				writer2.println(transactionInfo);
			}
			writer2.close();
			System.out.println("Success!");
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
