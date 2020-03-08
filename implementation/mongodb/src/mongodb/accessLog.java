/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package mongodb;

import com.mongodb.MongoClient;
import com.mongodb.client.MapReduceIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Scanner;
import org.bson.Document;

/**
 *
 * @author agandhi
 */
public class accessLog {
    
    //     ******** PART 5- PROGRAMMING ASSIGNMENT
        public static void main(String[] args) throws Exception{
//         program to read the access.log file
        MongoClient mongoClient= new MongoClient("localhost",27017);
        MongoDatabase database = mongoClient.getDatabase("bigdata");
        MongoCollection<Document> accesslogcollection = database.getCollection("access");
        File accessfile = new File("/Users/agandhi/Documents/bigdata/datasets/access.log"); 
        Scanner sc = new Scanner(accessfile);
        while(sc.hasNextLine()){
            String s = sc.nextLine();
            String ip = s.split(" - - ")[0];
            s= s.split(" - - \\[")[1];
            String datestr = s.split("\\]")[0];
            SimpleDateFormat formatter=new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss -SSSS");
            Date date = formatter.parse(datestr);
            s = s.split("\\]")[1].trim();
            String webpage = s.split("\"")[1];
            Document document = new Document("ip", ip)
                    .append("date",date)
            .append("website", webpage);
            accesslogcollection.insertOne(document);
            
        }
        
        
//         1.Number of times any webpage was visited by the same IP address. 
        String mapaccessfunction = "function() { emit(this.ip,1); }";
        String reduceaccessfunction = "function(key, values) { return Array.sum(values); }";
        MapReduceIterable iterablec = accesslogcollection.mapReduce(mapaccessfunction, reduceaccessfunction); // inline by default
         MongoCursor cursoraccess = iterablec.iterator();
        while (cursoraccess.hasNext()) {
           Document result = (Document)cursoraccess.next();
           System.out.println("Ip address : " + result.getString("_id") + ","
                   + " Total number of webpages visited: " + result.getDouble("value").intValue());

            
        }
        
//        2. Number of times any webpage was visited each month 
        String mapfunctionmonthly = "function() { emit(this.date.getMonth(),1); }";
        String reducefunctionmonthly = "function(key, values) { return Array.sum(values); }";
        MapReduceIterable iterablem = accesslogcollection.mapReduce(mapfunctionmonthly, reducefunctionmonthly); // inline by default
         MongoCursor cursormonthly = iterablem.iterator();
        while (cursormonthly.hasNext()) {
           Document result = (Document)cursormonthly.next();
           System.out.println("month : " + result.get("_id") + ","
                   + " Total number of webpages : " + result.getDouble("value").intValue());

            
  }

    
}}
