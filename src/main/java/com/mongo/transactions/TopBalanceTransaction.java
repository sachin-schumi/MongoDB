package com.mongo.transactions;

import com.mongodb.BasicDBObject;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;


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

			BasicDBObject andQuery = new BasicDBObject();
			List<BasicDBObject> obj = new ArrayList<BasicDBObject>();
			customer_collection.find().sort(new BasicDBObject("c_balance", -1)).limit(10);
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
}