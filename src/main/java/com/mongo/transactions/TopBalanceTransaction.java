package com.mongo.transactions;

import com.mongodb.BasicDBObject;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.sun.org.apache.xml.internal.security.algorithms.implementations.IntegrityHmac;
import com.sun.org.apache.xpath.internal.operations.Bool;
import org.bson.Document;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.mongo.transactions.LocalCache.customer_map;
import static com.mongo.transactions.LocalCache.district_map;
import static com.mongo.transactions.LocalCache.warehouse_map;


public class TopBalanceTransaction {

	static MongoCollection customer_collection;

	public TopBalanceTransaction(MongoDatabase session)
	{
		customer_collection = session.getCollection("customer");
	}

	public void getTopBalance(PrintWriter printWriter)
	{
		try
		{
            Map<Integer,Boolean> c_ids = new HashMap<Integer,Boolean>();
            int cnt = 0;
            MongoCursor<Document> items_cursor = customer_collection.find().sort(new BasicDBObject("c_balance", -1)).iterator();
            while(items_cursor.hasNext())
            {
                Document next = items_cursor.next();
                int c_id = (Integer)next.get("c_id");
                if(c_ids.get(c_id) == null && cnt <=10)
                {
                    c_ids.put(c_id,true);
                    printWriter.write("Top Balance Transaction--------"+"\n");
                    String customer_name = customer_map.get(c_id+"").split(",")[0];
                    String id = next.getInteger("c_w_id") + ","+next.getInteger("c_d_id");
                    String[] warehouseStaticInfo = warehouse_map.get(next.getInteger("c_w_id")+"").split(",");
                    String[] districtStaticInfo = district_map.get(id).split(",");
                    printWriter.write("Customer name : " + customer_name
                            + "| Customer Balance : "+ next.getDouble("c_balance") +" | Warehouse name "+warehouseStaticInfo[0]
                            + "| District name "+districtStaticInfo[0]);
                    cnt++;
                }
                if(cnt == 10)
                    break;
            }
        }
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
}