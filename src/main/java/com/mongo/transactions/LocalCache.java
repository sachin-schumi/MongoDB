package com.mongo.transactions;

import com.mongo.MongoSession;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Created by Sachin on 10/22/2016.
 */
public class LocalCache {

    public static Map<String,Integer> d_next_oid_map = new HashMap<String,Integer>();

    public void loadIntoCache()
    {
        try {
            Properties properties = Props.loadProps();
            MongoDatabase sesion = MongoSession.getSession(properties);

            MongoCollection table = sesion.getCollection("items");
            MongoCursor<Document> cursor = table.find().iterator();
            while(cursor.hasNext()) {
                Document next = cursor.next();
                System.out.println(next.get("_id"));
                Object _id = next.get("_id");
                Integer item_id = (Integer) next.get("i_id") ;
                //items.put(item_id,_id);
            }
            cursor.close();
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }

}
