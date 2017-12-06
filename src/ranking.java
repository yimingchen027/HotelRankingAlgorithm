package hotelRanking;

import java.net.UnknownHostException;

/*This class operates on a Mongo Database Collections*/
/*Currently, it has some simple functions for hotel ranking*/

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.MongoClient;
import com.mongodb.DBObject;

public class MongoOperations {
	/*Client object to connect to Mongo server*/
	public MongoClient mongoClient;
	/*The city we need to rank*/
	public String city;
	/*Mongo database object*/
	public DB db;
	/*Mongo collection object*/
	public DBCollection coll;
	/*C parameter in the function to calculate final score*/
	public double paraC;
	/*N(0.9) parameter in the function to calculate final score*/
	/*This parameter says we can trust the trust_score if review_counts is bigger than trust_review_count*/
	public double trust_review_count;
	
	public MongoOperations(String in_db_name, String in_coll_name, String in_city) throws UnknownHostException{
		mongoClient = new MongoClient("localhost", 27017);
		city = in_city;
		db = mongoClient.getDB(in_db_name);
		coll = db.getCollection(in_coll_name);
		computeFuncPara();
	}
	
	/*resetFinalScore: set all the hotels' final score to -1 in a city*/
	public void resetFinalScore(){
		Double fscore = new Double(-1);
		BasicDBObject newDocument = new BasicDBObject();
		newDocument.append("$set", new BasicDBObject().append("final_score", fscore));
	 
		BasicDBObject searchQuery = new BasicDBObject().append("city", city);
		
	 
		coll.update(searchQuery, newDocument, false, true);
	}
	
	/*refreshScore: re-compute the final score of all the hotels in this collection*/
	public void refreshScore(){		
		DBCursor cursor = coll.find();
		try {
		   while(cursor.hasNext()) {
			   BasicDBObject obj = (BasicDBObject) cursor.next();
			   Integer tscore = (Integer)obj.get("trust_score");
			   Integer reviewers = (Integer)obj.get("reviews_count");
			   Double fscore = new Double(-1);
			   if(tscore!=null){
				   fscore = calculateScore(tscore, reviewers);
				   
					BasicDBObject newDocument = new BasicDBObject();
					newDocument.append("$set", new BasicDBObject().append("final_score", fscore));

					coll.update(obj, newDocument);
				   
				   obj.append("final_score", fscore);
			   }
			   //System.out.println("trust score is: "+tscore+", reviewers is: "+ reviewers+", fscore is: "+fscore);
			   
		   }
		} finally {
		   cursor.close();
		}
		
	}
	
	/*sortAndPrint: this function sorts the hotels according to their final score, print the first rank_no hotels*/
	public void sortAndPrint(int rank_no){	
		DBObject sort = new BasicDBObject();
		sort.put("final_score", -1);
		DBCursor cursor = coll.find().sort(sort);
		int i = 0;
		String result = "";
	    while(cursor.hasNext() && i<rank_no){
	    	BasicDBObject obj = (BasicDBObject) cursor.next();
			Double fscore = (Double)obj.get("final_score");
			if(fscore == -1){
				break;
			}
			result += (i+1);
	    	result += "\t";
	    	result += printHotel(obj);
	    	i++;
	    }
	    cursor.close();
	    
	    System.out.println(result);
	    
	    if(i<rank_no) System.out.println("not enough hotels to rank top " + rank_no);
	}
	
	/*calculateScore: This function calculates the final score based on trust_score and review_conuts*/
	public double calculateScore(int trust_score, int reviewers){
		double weight = 1-Math.pow((reviewers+1), -paraC);
		double score = weight * trust_score;
		return score;
	}
	
	/*printHotel: This function retrieves hotel information and put them in a String*/
	public String printHotel(BasicDBObject hotel){
		String result;
		String name = (String)hotel.get("name");
		String cluster_id = (String)hotel.get("cluster_id");
		Integer tscore = (Integer)hotel.get("trust_score");
		Integer reviewers = (Integer)hotel.get("reviews_count");
		Double fscore = (Double)hotel.get("final_score");
		int fs = fscore.intValue();
		
		result = name + "\t\t"+cluster_id+ "\ttrust_score: "+ tscore + "       reviews count: " + reviewers+"\n";
		//result = name + "\ttrust_score: "+ tscore + "\treviews count: " + reviewers + "\tfinal score: " + fs + "\n";
		return result;		
	}
	
	
	/*computeFuncPara: this function computes the parameters for calculating the final score*/
	public void computeFuncPara(){
		DBObject sort = new BasicDBObject();
		sort.put("reviews_count", -1);
		DBCursor cursor = coll.find().sort(sort);
		int top10percent = (int) (coll.count() * 0.1);
		int i = 0;
		int total_count = 0;
	    while(cursor.hasNext() && i<top10percent){
	    	BasicDBObject obj = (BasicDBObject) cursor.next();
			int re_count = (Integer)obj.get("reviews_count");
			total_count+=re_count;
	    	i++;
	    }
	    int average = (int)(total_count/top10percent);
	    //System.out.println("average review counts for top 10% hotels in "+city + " is "+ average);
	    cursor.close();
	    
	    trust_review_count = average/50.7 - 5.15;
	    paraC = Math.log(10)/Math.log(trust_review_count+1);
		
	}

}
