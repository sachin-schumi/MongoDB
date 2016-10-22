package com.mongo.transactions;

import com.mongo.MongoSession;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import javax.print.Doc;
import java.util.*;

/**
 * Created by Sachin on 10/22/2016.
 */
public class LocalCache {

    public static Map<String,Integer> d_next_oid_map = new HashMap<String,Integer>();
    public static Map<Integer,Double> items_map = new HashMap<Integer,Double >();
    public static Map<String,Double> stocks_map = new HashMap<String,Double >();

    public void loadIntoCache()
    {
        try {
            Properties properties = Props.loadProps();
            MongoDatabase sesion = MongoSession.getSession(properties);


            MongoCollection table = sesion.getCollection("items");
            MongoCursor<Document> cursor = table.find().iterator();
            while(cursor.hasNext()) {
                Document next = cursor.next();
                Integer item_id = (Integer) next.get("i_id") ;
                Double price = (Double)next.get("i_price");
                items_map.put(item_id,price);
            }
            cursor.close();


            table = sesion.getCollection("warehouse");
            cursor = table.find().iterator();
            while(cursor.hasNext()) {
                Document next = cursor.next();
                Integer w_id = (Integer) next.get("w_id") ;
                ArrayList districts  = (ArrayList) next.get("district");

                for (Object district :districts)
                {
                    Document doc = (Document)district;
                    Integer d_id = (Integer)  doc.get("d_id");
                    Integer d_next_o_id = (Integer) doc.get("d_next_o_id");
                    d_next_oid_map.put(w_id+","+d_id,d_next_o_id);
                }
            }
            cursor.close();


            table = sesion.getCollection("stock");
            cursor = table.find().iterator();
            while(cursor.hasNext()) {
                Document next = cursor.next();
                Integer s_w_id = (Integer) next.get("s_w_id") ;
                Integer item_id = (Integer) next.get("s_i_id") ;
                Double quantity = (Double) next.get("s_quantity") ;
                String id = s_w_id + ","+item_id;
                stocks_map.put(id,quantity);
            }
            cursor.close();

        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }

}
