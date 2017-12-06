package hotelRanking;

import java.net.UnknownHostException;
import java.util.Scanner;


public class testMongo {
	public static void main(String args[]) throws UnknownHostException{
		System.out.println("Please enter a city name Berkeley or San Francisco");
		Scanner in=new Scanner(System.in);
		String readLine = in.nextLine(); 
		String coll="";
		String city="";
		if(readLine.equalsIgnoreCase("Berkeley"))
		{
			coll = "Berkeley";
			city = "Berkeley";		
		}
		else if(readLine.equalsIgnoreCase("San Francisco")){
			coll = "SanFrancisco";
			city = "San Francisco";
		}
		else{
			System.out.println("city name not recognized");
			System.exit(0);
		}
		MongoOperations mongo= new MongoOperations("hotelRanking", coll, city);
		mongo.resetFinalScore();
		mongo.refreshScore();
		mongo.sortAndPrint(50);
	}

}
