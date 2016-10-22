package com.mongo.transactions;

import java.io.PrintWriter;
import com.mongodb.BasicDBObject;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;


/**
 * Created by sachin on 22/10/16.
 */

public class PaymentTransaction {
    static  MongoCollection warehouse_collection ;
    static MongoCollection customer_collection;

    public PaymentTransaction(MongoDatabase session)
    {
        warehouse_collection = session.getCollection("warehouse");
        customer_collection = session.getCollection("customer");
    }

    public void setPayment(int w_id, int d_id, int c_id, double payment,PrintWriter printWriter) {
        try {

            BasicDBObject newDocument = new BasicDBObject().append("$inc",
                    new BasicDBObject().append("w_ytd", payment));
            warehouse_collection.updateOne(new BasicDBObject().append("w_id", w_id)
                    , newDocument);

            newDocument = new BasicDBObject().append("$inc",
                    new BasicDBObject().append("district.$.d_ytd", payment));
            warehouse_collection.updateOne(new BasicDBObject().append("w_id", w_id).append("district.d_id", d_id)
                    , newDocument);

            newDocument = new BasicDBObject().append("$inc",
                    new BasicDBObject().append("c_balance", payment).append("c_ytd_payment",payment)
                            .append("c_payment_cnt",payment));
            customer_collection.updateMany(new BasicDBObject().append("c_w_id", w_id).append("c_d_id", d_id).append("c_id", c_id)
                    , newDocument);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}