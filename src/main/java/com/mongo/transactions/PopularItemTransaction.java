package com.mongo.transactions;

import com.mongodb.BasicDBObject;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.io.PrintWriter;
import java.util.*;

import static com.mongo.transactions.LocalCache.customer_map;
import static com.mongo.transactions.LocalCache.d_next_oid_map;
import static com.mongo.transactions.LocalCache.items_name_map;

public class PopularItemTransaction {


    static MongoCollection orders_collection ;
    static MongoCollection stocks_collection;

    public PopularItemTransaction(MongoDatabase session)
    {
        orders_collection = session.getCollection("orders");
        stocks_collection = session.getCollection("stock");
    }

    public void checkPopularItem(int w_id, int d_id, int num_last_orders, PrintWriter printWriter) {
        try {

            String next_id = w_id + "," + d_id;
            int d_next_oid = 0;
            d_next_oid = d_next_oid_map.get(next_id);
            int start_index = d_next_oid - num_last_orders;
            BasicDBObject andQuery = new BasicDBObject();
            List<BasicDBObject> obj = new ArrayList<BasicDBObject>();
            obj.add(new BasicDBObject("o_w_id", w_id));
            obj.add(new BasicDBObject("o_d_id", d_id));
            obj.add(new BasicDBObject("o_id", new BasicDBObject("$gte", start_index).append("$lte", d_next_oid)));
            andQuery.put("$and", obj);

            printWriter.write("POPULAR ITEM TRANSACTION--------" + "\n");
            printWriter.write("(W_ID, D_ID, NUM_OF_LAST_ORDER_TO_BE_EXAMINED)(" + w_id + ", " + d_id + ", " + num_last_orders + ")\n");

            MongoCursor<Document> cursor = orders_collection.find(andQuery).iterator();
            Map<Integer, List<Integer>> orderItemsMapping = new HashMap<Integer, List<Integer>>();
            Set<Integer> itemids = new HashSet<Integer>();
            while (cursor.hasNext()) {
                Document next = cursor.next();
                ArrayList orders = (ArrayList) next.get("o_items");
                Date o_entry_d = (Date)next.get("o_entry_d");
                int order_id = (Integer) next.get("o_id");
                int c_id = (Integer)next.get("o_c_id");
                String customer_name = customer_map.get(c_id+"").split(",")[0];
                printWriter.write("(O_ID, O_ENTRY_D, CUST_NAME)(" + order_id + ", " + o_entry_d + ", " + customer_name + "\n");
                for (Object order : orders) {
                    Document d = (Document) order;
                    int itemId = (Integer) d.get("i_id");
                    itemids.add(itemId);
                    List<Integer> items = null;
                    if (orderItemsMapping.get(order_id) == null) {
                        items = new ArrayList<Integer>();
                    } else {
                        items = orderItemsMapping.get(order_id);
                    }
                    items.add(itemId);
                    orderItemsMapping.put(order_id, items);
                }
            }
            andQuery = new BasicDBObject();
            obj = new ArrayList<BasicDBObject>();
            obj.add(new BasicDBObject("s_w_id", w_id));
            obj.add(new BasicDBObject("s_i_id", new BasicDBObject("$in",itemids)));
            andQuery.put("$and", obj);
            MongoCursor<Document>  items_cursor = stocks_collection.find(andQuery).iterator();

            Map<Integer, Double> orderItemQuantity = new HashMap<Integer, Double>();
                while (items_cursor.hasNext()) {
                   Document next = items_cursor.next();
                    int item_id = (Integer) next.get("s_i_id");
                    double quantity = next.getDouble("s_quantity");
                    orderItemQuantity.put(item_id, quantity);
                }

                Map<Integer, List<Integer>> itemOrdersMap = new HashMap<Integer, List<Integer>>();
                for (Map.Entry<Integer, List<Integer>> entry : orderItemsMapping.entrySet()) {
                    for (Integer itemid : entry.getValue()) {
                        List<Integer> items = null;
                        if (itemOrdersMap.get(itemid) == null) {
                            items = new ArrayList<Integer>();
                        } else {
                            items = itemOrdersMap.get(itemid);
                        }
                        items.add(entry.getKey());
                        itemOrdersMap.put(itemid, items);
                    }
                }
                //Iterate over order-item map and get max item id for each order
                for (Map.Entry<Integer, List<Integer>> entry : orderItemsMapping.entrySet()) {
                    int popularItem = getMaxQuantity(entry.getValue(), orderItemQuantity);
                    printWriter.write("(ITEM_NAME, ITEM_QUANTITY, PERCENTAGE OF ORDERS IN S THAT CONTAINS THIS ITEM)" +
                            "(" + items_name_map.get(popularItem) + "," + orderItemQuantity.get(popularItem)
                            + ", " + ", " + ((itemOrdersMap.get(popularItem).size() / orderItemsMapping.size()) * 100) + "%)\n");
                }
            printWriter.write("\n");
            printWriter.flush();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    //Returns max item id
    private int getMaxQuantity(List<Integer> value, Map<Integer, Double> orderItemQuantity) {
        Iterator<Integer> it = value.iterator();
        double max = Integer.MIN_VALUE;
        int max_item_id = 0;
        while (it.hasNext()) {
            int item_id = it.next();
            double item_quantity = orderItemQuantity.get(item_id);
            if (item_quantity > max) {
                max = item_quantity;
                max_item_id = item_id;
            }
        }
        return max_item_id;
    }
}