package com.mongo.transactions;

import com.mongodb.BasicDBObject;
import com.mongodb.QueryBuilder;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.*;

import static com.mongo.transactions.LocalCache.d_next_oid_map;

/**
 * Created by ritesh on 09/10/16.
 */
public class NewOrderTransaction {

    static List<String> columns = Arrays.asList("o_w_id", "o_d_id", "o_id", "o_c_id", "o_entry_d", "o_carrier_id", "o_ol_cnt", "o_all_local", "o_items");
    static final String[] columns_next_order = {"no_d_next_o_id"};

    public void newOrderTransaction(int w_id, int d_id, int c_id, ArrayList<String> itemlineinfo, MongoDatabase session,PrintWriter printWriter) {
        try {
            // put the order in order status trasaction
            // put the order status transaction
            // update customer data
            // update next order
            // update stock level


            String next_id = w_id+""+d_id;
            int d_next_oid = 0;
            d_next_oid = d_next_oid_map.get(next_id) + 1;
            d_next_oid_map.put(w_id+""+d_id,d_next_oid);


            BasicDBObject searchQuery = new BasicDBObject().append("no_w_id", w_id).append("no_d_id",d_id);


            BasicDBObject newDocument = new BasicDBObject();
            newDocument.append("$set", new BasicDBObject().append("d_next_oid", d_next_oid));

            MongoCollection collection = session.getCollection("warehouse");
            collection.updateOne(searchQuery, newDocument);

            String dNextOIDUpdate = "update next_order set no_d_next_o_id = "+d_next_oid+" where no_w_id ="+w_id+"" +
                    " and no_d_id = "+d_id;
            //session.execute(dNextOIDUpdate);


            /*
            String getDNextOID = "select no_d_next_o_id from next_order where no_w_id =" + 1 + " and no_d_id = 1";
            ResultSet results = session.execute(getDNextOID);
            int d_next_oid = results.one().getInt("no_d_next_o_id");
            String dNextOIDUpdate = "update next_order set no_d_next_o_id = " + (d_next_oid + 1) + " where no_w_id =" + w_id + " and no_d_id = " + d_id;
            */
            List<Object> values = new ArrayList<Object>();
            values.add(w_id);
            values.add(d_id);
            values.add(d_next_oid + 1);
            values.add(c_id);
            Date orderEntryDate = new Date();
            values.add(orderEntryDate);
            values.add(null);
            values.add(itemlineinfo.size());

            double all_local = 1;
            int itemnum = 0;

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}