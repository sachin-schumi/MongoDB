package com.mongo.transactions;

import com.mongodb.BasicDBObject;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.util.JSON;
import org.bson.Document;
import java.io.PrintWriter;
import java.util.*;

import static com.mongo.transactions.LocalCache.*;

/**
 * Created by sachin on 22/10/16.
 */
public class NewOrderTransaction {

    static MongoCollection stock_collection ;
    static MongoCollection warehouse_collection;
    static MongoCollection orders_collection;

    public NewOrderTransaction(MongoDatabase session)
    {
        stock_collection = session.getCollection("stock");
        warehouse_collection = session.getCollection("warehouse");
        orders_collection = session.getCollection("orders");
    }

    public void newOrderTransaction(int w_id, int d_id, int c_id, ArrayList<String> itemlineinfo,MongoDatabase session,PrintWriter printWriter) {
        try {
            // put the order in order status trasaction
            // put the order status transaction
            // update customer data
            // update next order
            // update stock level
            String next_id = w_id+","+d_id;
            int d_next_oid = 0;
            d_next_oid = d_next_oid_map.get(next_id);
            d_next_oid_map.put(next_id,d_next_oid+ 1);

            BasicDBObject newDocument = new BasicDBObject().append("$inc",
                    new BasicDBObject().append("district.$.d_next_o_id", 1));
            warehouse_collection.updateOne(new BasicDBObject().append("w_id", w_id).append("district.d_id", d_id)
                    , newDocument);

            int itemnum = 0;
            Set<Object> items = new HashSet<Object>();
            double total_amount = 0.0;
            double all_local = 1;
            StringBuilder sb = new StringBuilder();
            for (String item : itemlineinfo) {
                String[] itemline = item.split(",");
                //Order line info update
                int ol_i_id = Integer.parseInt(itemline[0]);
                int ol_supply_w_id = Integer.parseInt(itemline[1]);
                double ol_quantity = Double.parseDouble(itemline[2]);
                double itemPrice = items_price_map.get(ol_i_id);
                double ol_amount = itemPrice * ol_quantity;
                String itemString = "{\"i_id\":"+ol_i_id + ",\"ol_number\":"+ (++itemnum) + ",\"supply_w_id\" :"+ ol_supply_w_id +
                        ",\"i_quantity\":"+ ol_quantity +",\"i_amount\":"+ol_amount+",\"i_delivery_d\" :"+ null+",\"i_dist_info\":\"\""+" }";
                Object eachItem = JSON.parse(itemString);
                items.add(eachItem);
                total_amount += ol_amount;
                if (ol_supply_w_id != w_id) {
                    all_local = 0;
                }
                //Stock info update
                double stockQuantity = LocalCache.stocks_map.get(w_id+","+ol_i_id);
                double adjustedQuantiy = stockQuantity - ol_quantity;
                if (adjustedQuantiy < 10) {
                    adjustedQuantiy = adjustedQuantiy + 100;
                }
                stocks_map.put(w_id+","+ol_i_id,adjustedQuantiy);
                newDocument = new BasicDBObject().append("$inc",
                        new BasicDBObject().append("s_quantity", -(stockQuantity-adjustedQuantiy) )
                                .append("s_ytd", stockQuantity).append("s_order_cnt",1).append("s_remote_cnt",1));
               stock_collection.updateOne(new BasicDBObject().append("s_w_id", w_id).append("s_i_id", ol_i_id)
                        , newDocument);

                // print info
                sb.append("(ITEM_NUMBER " + itemnum + " | ");
                sb.append("ITEM_NAME " + items_name_map.get(ol_i_id) + " | ");
                sb.append("SUPPLIER_WAREHOUSE " + ol_supply_w_id + " | ");
                sb.append("QUANTITY " + ol_quantity + " | ");
                sb.append("OL_AMOUNT " + ol_amount + " | ");
                sb.append("S_QUANTITY " + adjustedQuantiy +")"+ "\n");
            }
            Document orders = new Document();
            orders.put("o_w_id", w_id);
            orders.put("o_d_id", d_id);
            orders.put("o_c_id",c_id);
            orders.put("o_id", d_next_oid);
            Date orderEntryDate = new Date();
            orders.put("o_entry_d", orderEntryDate);
            orders.put("o_carrier_id", null);
            orders.put("o_ol_cnt", itemlineinfo.size());
            orders.put("o_all_local", all_local);
            orders.put("o_items",items);
            orders_collection.insertOne(orders);


            String[] warehouseStaticInfo = warehouse_map.get(w_id+"").split(",");
            String[] districtStaticInfo = district_map.get(w_id+","+d_id).split(",");
            String[] customerStaticInfo = customer_map.get(c_id+"").split(",");

            total_amount = total_amount * (1 + Double.parseDouble(warehouseStaticInfo[2])
                    + Double.parseDouble(districtStaticInfo[2]))
                    * (1 - Double.parseDouble(customerStaticInfo[6]));
            printWriter.write("NEW ORDER TRANSACTION--------" + "\n");
            // (W ID, D ID, C ID), lastname C LAST, credit C CREDIT, discount C DISCOUNT
            printWriter.write("Customer Identifier: (W ID, D ID, C ID), C_LAST, C_CREDIT, C_DISCOUNT)(" + w_id + ", " + d_id + ", " + c_id + ") "
                    + customerStaticInfo[0] + ", " + customerStaticInfo[4] + ", " + customerStaticInfo[6] + "\n");
            // Warehouse tax rate W TAX, District tax rate D TAX
            printWriter.write("(W_TAX, D_TAX)(" + warehouseStaticInfo[2] + ", " + districtStaticInfo[2] + ")\n");
            // Order number O ID, entry date O ENTRY D
            printWriter.write("(O_ID, O_ENTRY_D)(" + (d_next_oid+1) + ", " + orderEntryDate + ")\n");
            // Number of items NUM ITEMS, Total amount for order TOTAL AMOUNT
            printWriter.write("(NUM_ITEMS, TOTAL_AMOUNT)(" + items.size() + ", " + total_amount + "\n");
            printWriter.write(sb.toString());
            printWriter.write("\n");
            printWriter.flush();


        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}