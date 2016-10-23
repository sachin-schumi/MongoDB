package com.mongo.transactions;

import java.io.PrintWriter;
import java.util.*;
import java.util.List;
import com.mongodb.BasicDBObject;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import static com.mongo.transactions.LocalCache.d_next_oid_map;

public class StockLevelTransaction {
    static MongoCollection orders_collection ;
    static MongoCollection stocks_collection;

    public StockLevelTransaction(MongoDatabase session)
    {
        orders_collection = session.getCollection("orders");
        stocks_collection = session.getCollection("stock");
    }

    public void checkStockThreshold(int w_id, int d_id, double threshold, int num_last_orders, PrintWriter printWriter) {
        try {
            String next_id = w_id+","+d_id;
            int d_next_oid = 0;
            d_next_oid = d_next_oid_map.get(next_id);
            int start_index = d_next_oid - num_last_orders;
            BasicDBObject andQuery = new BasicDBObject();
            List<BasicDBObject> obj = new ArrayList<BasicDBObject>();
            obj.add(new BasicDBObject("o_w_id", w_id));
            obj.add(new BasicDBObject("o_d_id", d_id));
            obj.add(new BasicDBObject("o_id", new BasicDBObject("$gte", start_index).append("$lte", d_next_oid)));
            andQuery.put("$and", obj);

            Map<Integer, List<Integer>> orderItemsMapping = new HashMap<Integer, List<Integer>>();

            MongoCursor<Document>  cursor = orders_collection.find(andQuery).iterator();

            while(cursor.hasNext())
            {
                Document next = cursor.next();
                ArrayList orders = (ArrayList) next.get("o_items");
                Set<Integer> itemids = new HashSet<Integer>();
                for(Object order : orders)
                {
                    Document d = (Document)order;
                    int itemId = (Integer) d.get("i_id");
                    itemids.add(itemId);
                }
                andQuery = new BasicDBObject();
                obj = new ArrayList<BasicDBObject>();
                obj.add(new BasicDBObject("s_w_id", w_id));
                obj.add(new BasicDBObject("s_i_id", new BasicDBObject("$in",itemids)));
                andQuery.put("$and", obj);
                MongoCursor<Document>  items_cursor = stocks_collection.find(andQuery).iterator();
                while(items_cursor.hasNext())
                {
                    next = cursor.next();
                    double quantity = next.getDouble("s_quantity");
                    if(quantity < threshold)
                        System.out.println("Less");
                }


            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}