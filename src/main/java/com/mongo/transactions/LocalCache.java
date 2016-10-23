package com.mongo.transactions;

import com.mongo.MongoSession;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import javax.print.Doc;
import java.util.*;

/**
 * Created by Sachin on 10/22/2016.
 */
public class LocalCache {

    public static Map<String,Integer> d_next_oid_map = new HashMap<String,Integer>();
    public static Map<Integer,String> items_name_map = new HashMap<Integer,String >();
    public static Map<String,Double> stocks_map = new HashMap<String,Double >();
    public static Map<Integer,Double> items_price_map = new HashMap<Integer,Double >();
    public static Map<String,String > customer_map = new HashMap<String,String >();
    public static Map<String,String > warehouse_map = new HashMap<String,String >();
    public static Map<String,String > district_map = new HashMap<String,String >();

    public void loadIntoCache()
    {
        try {
            Properties properties = Props.loadProps();
            MongoDatabase sesion = MongoSession.getSession(properties);

            MongoCollection table = sesion.getCollection("items");
            MongoCursor<Document> cursor = table.find().iterator();
            while(cursor.hasNext()) {
                Document next = cursor.next();
                Integer item_id = (Integer) next.get("i_id") ;
                String item_name = next.getString("i_name");
                Double price = (Double)next.get("i_price");
                items_price_map.put(item_id,price);
                items_name_map.put(item_id,item_name);
            }
            cursor.close();
            table = sesion.getCollection("customer");
            cursor = table.find().iterator();
            while(cursor.hasNext()) {
                Document next = cursor.next();
                Integer c_id = (Integer) next.get("c_id") ;
                String customer_name = next.getString("c_first") + " " + next.getString("c_middle")
                        + " " + next.getString("c_last");
                String customer_addr = next.getString("c_street_1") + " " + next.getString("c_street_2")
                        + " " + next.getString("c_city")+ " " + next.getString("c_state")+ " " + next.getString("c_zip");
                String customer_phone = next.getString("c_phone");
                String customer_since = next.getDate("c_since").toString();
                String customer_crd_st = next.getString("c_credit");
                String customer_crd_lim = next.getDouble("c_credit_lim")+"";
                String customer_discount = next.getDouble("c_discount")+"";
                customer_map.put(c_id+"",customer_name+","+customer_addr+","+customer_phone+","+customer_since
                        +","+customer_crd_st+","+customer_crd_lim+","+customer_discount);
            }
            cursor.close();

            table = sesion.getCollection("warehouse");
            cursor = table.find().iterator();
            while(cursor.hasNext()) {
                Document next = cursor.next();
                Integer w_id = (Integer) next.get("w_id") ;
                ArrayList districts  = (ArrayList) next.get("district");
                String w_name = next.getString("w_name");
                String w_address = next.getString("w_street_1") + " " + next.getString("w_street_2")
                        + " " + next.getString("w_city")+ " " + next.getString("w_state")+ " " + next.getString("w_zip");
                String w_tax = next.getDouble("w_tax")+"";
                warehouse_map.put(w_id+"",w_name+","+w_address+","+w_tax);
                for (Object district :districts)
                {
                    Document doc = (Document)district;
                    Integer d_id = (Integer)  doc.get("d_id");
                    String d_name = doc.getString("d_name");
                    String d_address = doc.getString("d_street_1") + " " + doc.getString("d_street_2")
                            + " " + doc.getString("d_city")+ " " + doc.getString("d_state")+ " " + doc.getString("d_zip");
                    String d_tax = doc.getDouble("d_tax")+"";
                    Integer d_next_o_id = (Integer) doc.get("d_next_o_id");
                    d_next_oid_map.put(w_id+","+d_id,d_next_o_id);
                    district_map.put(w_id+","+d_id,d_name+","+d_address+","+d_tax);
                }
            }
            cursor.close();

            table = sesion.getCollection("stock");
            cursor = table.find().iterator();
            while(cursor.hasNext()) {
                Document next = cursor.next();
                Integer s_w_id = (Integer) next.get("s_w_id") ;
                Integer item_id = (Integer) next.get("s_i_id") ;
                Double quantity = (Double) next.get("s_quantity") ;
                String id = s_w_id + ","+item_id;
                stocks_map.put(id,quantity);
            }
            cursor.close();
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }
}