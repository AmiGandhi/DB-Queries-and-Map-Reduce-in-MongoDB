/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package mongodb;

/**
 *
 * @author agandhi
 */
import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClients;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MapReduceIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import org.bson.Document;
import java.io.File;
import java.io.FileNotFoundException;
import java.text.SimpleDateFormat;
import java.util.Scanner;
import java.util.ArrayList;
import java.util.List;
import java.util.Date;

public class movieLens {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws Exception{

        MongoClient mongoClient= new MongoClient("localhost",27017);
         MongoDatabase database = mongoClient.getDatabase("bigdata");
         MongoCollection<Document> coll = database.getCollection("ratings");
         MongoCollection<Document> collm = database.getCollection("movies");
         MongoCollection<Document> collt = database.getCollection("tags");
         
         
//*****PROGRAMMING******
         //importing ratings data from ratings.dat
         try {
             
			Scanner scanner = new Scanner(new File("/Users/agandhi/Documents/bigdata/datasets/ml-10M100K/ratings.dat"));
			while (scanner.hasNextLine() ) {
                                String s=scanner.nextLine();
                                String[] row=s.split("::");
                                Document data= new Document("UserID",Integer.valueOf(row[0])).append("MovieID",Integer.valueOf(row[1])).append("Rating",Float.valueOf(row[2])).append("Timestamp",row[3]);
                                coll.insertOne(data);
							}
			scanner.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}

         
         
//         importing ratings data from tags.dat
                 File tags = new File("/Users/agandhi/Documents/bigdata/datasets/ml-10M100K/tags.dat"); 
        Scanner tagsscanner = new Scanner(tags);
        while(tagsscanner.hasNextLine()){
            String s = tagsscanner.nextLine();
            String[] sl = s.split("::");
            String userID = sl[0];
            String movieID = sl[1];
            String tag = sl[2];
            String timestamp = sl[3];
            Document document = new Document("userID", Integer.parseInt(userID)) 
            .append("movieID", Integer.parseInt(movieID))
            .append("tag", tag)
            .append("timestamp",timestamp);
            collt.insertOne(document); 

        }
//         importing movies data from movies.dat  
        File file = new File("/Users/agandhi/Documents/bigdata/datasets/ml-10M100K/movies.dat"); 
        Scanner sc = new Scanner(file);
        while(sc.hasNextLine()){
            String s = sc.nextLine();
            String[] sl = s.split("::");
            List<String> genre = new ArrayList<>();
            String[] namestr = sl[1].split("\\(");
            String name ="";
            Integer movieID = Integer.parseInt(sl[0]);
            int year=0;
            for(int i=0;i<namestr.length;i++){
                if(i == namestr.length-1){
                    year = Integer.parseInt(namestr[i].split("\\)")[0]);
                }
                else{
                    name =name+namestr[i];
                }
            }
            String[] genrearr = sl[2].split("\\|");
            for(String ss: genrearr) genre.add(ss);
            
            System.out.println(name+year);
            System.out.println(genre);
            Document document = new Document("name", name)
                    .append("movieID",movieID)
            .append("year", year)
            .append("genre", genre);
            collm.insertOne(document); 

        }
        
        
//         1. Number of Movies released per year (Movies Collection) 
        String mapfunction = "function() { emit(this.year, 1); }";
        String reducefunction = "function(key, values) { return Array.sum(values); }";
        MapReduceIterable iterable = collm.mapReduce(mapfunction, reducefunction); // inline by default
         MongoCursor cursor = iterable.iterator();

        while (cursor.hasNext()) {
           Document result = (Document)cursor.next();
           System.out.println("movieId: " + result.getDouble("_id").intValue() + ","
                   + " Total number of movies: " + result.getDouble("value").intValue());
        }
        
        
        
        
//       2.  Number of Movies per genre (Movies Collection) 
        String mapfunction2 = "function() { for(i=0;i<this.genre.length;i++){emit(this.genre[i],1);} }";
        String reducefunction2 = "function(key, values) { return Array.sum(values); }";
        MapReduceIterable iterable2 = collm.mapReduce(mapfunction2, reducefunction2); // inline by default
         MongoCursor cursor2 = iterable2.iterator();

        while (cursor2.hasNext()) {
           Document result = (Document)cursor2.next();
           System.out.println("genre: " + result.getString("_id") + ","
                   + " Total number of movies: " + result.getDouble("value").intValue());
        }
        
         
//        3. Number of Movies per rating (Ratings Collection) 
        String mapfunction3 = "function() { emit(this.Rating,1); }";
        String reducefunction3 = "function(key, values) { return Array.sum(values); }";
        MapReduceIterable iterable3 = coll.mapReduce(mapfunction3, reducefunction3); // inline by default
         MongoCursor cursor3 = iterable3.iterator();

        while (cursor3.hasNext()) {
           Document result = (Document)cursor3.next();
           System.out.println("rating : " + result.getDouble("_id") + ","
                   + " Total number of movies: " + result.getDouble("value").intValue());
        }
        
        
//        4.Number of times each movie was tagged (Tags Collection)
          String mapfunction4 = "function() { emit(this.movieID,1); }";
        String reducefunction4 = "function(key, values) { return Array.sum(values); }";
        MapReduceIterable iterable4 = collt.mapReduce(mapfunction4, reducefunction4); // inline by default
         MongoCursor cursor4 = iterable4.iterator();

        while (cursor4.hasNext()) {
           Document result = (Document)cursor4.next();
           System.out.println("movie ID : " + result.getDouble("_id") + ","
                   + " Total number tags: " + result.getDouble("value").intValue());
        }
    }
}