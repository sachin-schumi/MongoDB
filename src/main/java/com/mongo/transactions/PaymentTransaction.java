package com.mongo.transactions;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import com.mongodb.BasicDBObject;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import static com.mongo.transactions.LocalCache.customer_map;
import static com.mongo.transactions.LocalCache.district_map;
import static com.mongo.transactions.LocalCache.warehouse_map;


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


            String[] customerStaticInfo = customer_map.get(c_id+"").split(",");
            String output = "PAYMENT TRANSACTION--------" + "\n";

            //printWriter.write();
            output += "Customer Identifier : " + w_id + "" + d_id + "" + c_id +
                    " | Customer name : " + customerStaticInfo[0] +
                    " | Customer address : " + customerStaticInfo[1] + "\n" + "Customer phone : " + customerStaticInfo[2]
                    + "\n" + "Entry created date : " + customerStaticInfo[3] + " | Customer credit status : "
                    + customerStaticInfo[4] + "\n" + "Customer credit limit : " + customerStaticInfo[5]
                    + " | Customer discount rate : " + customerStaticInfo[6]  + " | Customer outstanding balance : "
                    + c_balance + "\n";


            String[] warehouseStaticInfo = warehouse_map.get(w_id +"").split(",");
            String[] districtStaticInfo = district_map.get(w_id + ","+d_id).split(",");

            output += "Warehouse address : " +warehouseStaticInfo[1] + "\n";
            output += "District address : " + districtStaticInfo[1] + "\n";
            output += "Payment amount : " + payment + "\n";
            printWriter.write(output + "\n");
            printWriter.write("\n");
            printWriter.flush();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}