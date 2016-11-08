package com.mongo.transactions;

import java.io.PrintWriter;
import java.util.*;
import com.mongodb.BasicDBObject;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

public class DeliveryTransaction {

    static MongoCollection orders_collection ;
    static MongoCollection customer_collection;

    public DeliveryTransaction(MongoDatabase session)
    {
        orders_collection = session.getCollection("orders");
        customer_collection = session.getCollection("customer");
    }
    public void readDeliveryTransaction(int w_id, int carrier_id, PrintWriter printWriter)
    {
        try
        {
            for(int i=1; i<=10; i++){
                BasicDBObject andQuery = new BasicDBObject();
                List<BasicDBObject> obj = new ArrayList<BasicDBObject>();
                obj.add(new BasicDBObject("o_w_id", w_id));
                obj.add(new BasicDBObject("o_d_id", i));
                obj.add(new BasicDBObject("o_carrier_id", -1));
                andQuery.put("$and", obj);
                FindIterable records = orders_collection.find(andQuery);
                MongoCursor<Document> cursor = records.sort(new BasicDBObject("o_id", 1)).limit(1).iterator();

                if(cursor.hasNext())
                {
                    Document next = cursor.next();
                    int order_id = (Integer) next.get("o_id");
                    int d_id = (Integer)next.get("o_d_id");
                    int c_id = (Integer)next.get("o_c_id");
                    ArrayList orders = (ArrayList) next.get("o_items");
                    double ol_amt_sum = 0;

                    for(int j=0; j<orders.size(); j++){
                        Document d = (Document) orders.get(j);
                        d.remove("i_delivery_d");
                        d.put("i_delivery_d",new Date());
                        ol_amt_sum += d.getDouble("i_amount");
                        orders.set(j,d);
                    }

                    BasicDBObject newDocument = new BasicDBObject().append("$set",
                            new BasicDBObject().append("o_carrier_id", carrier_id).append("o_items",orders));
                    orders_collection.updateOne(new BasicDBObject().append("o_w_id", w_id).append("o_d_id", d_id)
                            .append("o_id", order_id), newDocument);

                    newDocument = new BasicDBObject().append("$inc",
                            new BasicDBObject().append("c_balance", ol_amt_sum).append("c_delivery_cnt",1));
                    customer_collection.updateOne(new BasicDBObject().append("c_w_id", w_id).append("c_d_id", d_id)
                            .append("c_id", c_id), newDocument);
                }
            }
        }
        catch(Exception e)
        {
            e.printStackTrace();
        }
    }
}