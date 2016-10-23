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

import static com.mongo.transactions.LocalCache.customer_map;

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
            obj.add(new BasicDBObject("c_w_id", w_id));
            obj.add(new BasicDBObject("c_d_id", d_id));
            obj.add(new BasicDBObject("c_id", c_id));
            andQuery.put("$and", obj);
            double c_balance = 0.0;
            MongoCursor<Document> cust_records = customer_collection.find(andQuery).iterator();
            if(cust_records.hasNext())
            {
                c_balance = cust_records.next().getDouble("c_balance");
            }
            andQuery = new BasicDBObject();
            obj = new ArrayList<BasicDBObject>();
            obj.add(new BasicDBObject("o_w_id", w_id));
            obj.add(new BasicDBObject("o_d_id", d_id));
            obj.add(new BasicDBObject("o_c_id", c_id));
            andQuery.put("$and", obj);
            FindIterable records = orders_collection.find(andQuery);
            records.sort(new BasicDBObject("o_id", -1)).limit(1);
            MongoCursor<Document> cursor = records.iterator();
            while (cursor.hasNext()) {
                Document next = cursor.next();
                Integer o_id = (Integer) next.get("o_id") ;
                String customer_name = customer_map.get(c_id+"").split(",")[0];
                printWriter.write("ORDER STATUS TRANSACTION--------" + "\n");
                printWriter.write("Customer name : " + customer_name + "\n");
                printWriter.write("Customer balance : " + c_balance + "\n");
                ArrayList o_items = (ArrayList)next.get("o_items");
                for(Object item : o_items) {
                    Document doc = (Document)item;
                    printWriter.write("Item number: " + doc.getInteger("i_id") + " | "
                            + "Warehouse number: " + doc.getInteger("supply_w_id")
                            + " | " + "Quantity number: " + doc.get("i_quantity")
                            + " | " + "Total price: " + doc.getDouble("i_amount")
                            + " | " + "Date and time of delivery: " + doc.getString("i_delivery_d") + "\n");
                }
            }
            printWriter.write("\n");
            printWriter.flush();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}