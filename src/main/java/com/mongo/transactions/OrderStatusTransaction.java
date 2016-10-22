package com.mongo.transactions;

import com.mongodb.BasicDBObject;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.bson.codecs.DoubleCodec;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

public class OrderStatusTransaction {

    static MongoCollection orders_collection ;
    static MongoCollection customer_collection;

    public OrderStatusTransaction(MongoDatabase session)
    {
        orders_collection = session.getCollection("orders");
        customer_collection = session.getCollection("customer");
    }

    public void readOrderStatus(int w_id, int d_id, int c_id,PrintWriter printWriter) {
        try {
            BasicDBObject andQuery = new BasicDBObject();
            List<BasicDBObject> obj = new ArrayList<BasicDBObject>();
            obj.add(new BasicDBObject("o_w_id", w_id));
            obj.add(new BasicDBObject("o_d_id", d_id));
            obj.add(new BasicDBObject("o_c_id", c_id));
            andQuery.put("$and", obj);
            FindIterable records = orders_collection.find(andQuery);
            records.sort(new BasicDBObject("o_id", -1)).limit(1);
            MongoCursor<Document> cursor = records.iterator();
            while (cursor.hasNext()) {
                Document next = cursor.next();
                Double o_id = (Double) next.get("o_id") ;
                System.out.println(o_id);
                ArrayList o_items = (ArrayList)next.get("o_items");
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}