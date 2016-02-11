package DBproject1;

import java.util.Random;

public class GenerateData {
	
	private Random rand;

	public GenerateData(){
		rand = new Random();
	}
	
	/**
	 * Generate a integer randomly
	 * @param min
	 * @param max
	 * @return
	 */
	public int generateInteger(int min, int max){
		int randNumber = rand.nextInt(max-min+1) + min;
		return randNumber;
	}
	
	/**
	 * Generate a sequence of characters randomly
	 * @param minLength
	 * @param maxLength
	 * @return
	 */
	public String generateCharacters(int minLength, int maxLength){
		int strLength = generateInteger(minLength, maxLength);
		char c[] = new char[strLength];
		for(int i = 0; i < strLength; i++){
			int ascNumber = generateInteger(40,127);
			if(ascNumber!=44){
				c[i] = (char)ascNumber;
			}else{
				c[i] = (char)(ascNumber+1);
			}
		}
		return c.toString();
	}
	
	/**
	 * Generate a float randomly
	 * @param min
	 * @param max
	 * @return
	 */
	public float generateFloat(float min, float max){
		
		float randNumber = rand.nextFloat()*(max-min) + min;
		return randNumber;
	}
	
	public String generateName(int minLength, int maxLength){
		int strLength = generateInteger(minLength, maxLength);
		
		int firstNameLength = minLength/2;
		
		int lastNameLength = strLength - firstNameLength - 1; // one for space
		
		char firstName [] = new char[firstNameLength];
		char lastName [] = new char[lastNameLength];
		
		for(int i = 0; i < firstNameLength; i ++){
			int ascNumber = generateInteger(97,122);
			firstName[i] = (char)ascNumber;
		}
		for(int i = 0; i < lastNameLength; i ++){
			int ascNumber = generateInteger(97,122);
			lastName[i] = (char)ascNumber;
		}
		firstName[0] = Character.toUpperCase(firstName[0]);
		lastName[0] = Character.toUpperCase(lastName[0]);
		
		
		return new String(firstName) + " " + new String(lastName);
		
	}
}
