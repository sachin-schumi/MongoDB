package com.mongo.transactions;

import java.io.File;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.mongodb.client.MongoDatabase;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;


public class PaymentTransaction {
    static String ytdcolums[] = {"no_w_ytd", "no_d_ytd"};
    static String customercolums[] = {"c_balance", "c_ytd_payment", "c_payment_cnt"};

    public void readOrderStatus(int w_id, int d_id, int c_id, double payment, MongoDatabase session,PrintWriter printWriter) {
        try {
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}