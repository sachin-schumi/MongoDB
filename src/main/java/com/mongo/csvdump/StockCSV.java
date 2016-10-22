package com.mongo.csvdump;

import com.mongodb.*;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.opencsv.CSVReader;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.bson.Document;

import java.io.*;
import java.util.*;

/**
 * Created by sachin on 21/10/2016.
 */
public class StockCSV {
    private static Logger logger = Logger.getLogger(StockCSV.class);

    private Map<Integer,Object> items = new HashMap<Integer,Object>();

    public void getItems()
    {
        try {
            MongoClient mongo = new MongoClient("localhost", 27017);

            MongoDatabase db = mongo.getDatabase("test");
            MongoCollection table = db.getCollection("items");
            MongoCursor<Document> cursor = table.find().iterator();
            while(cursor.hasNext()) {
                Document next = cursor.next();
                System.out.println(next.get("_id"));
                //Object _id = next.get("_id");
                //Integer item_id = (Integer) next.get("i_id") ;
                //items.put(item_id,_id);
            }
            cursor.close();
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }

    public void prepareCsv(Properties properties) {
        String csv_dump_path = properties.getProperty("csv_dump_path");
        String csv_files_path = properties.getProperty("csv_files_path");
        PrintWriter pw = null;
        logger.info("Preparing csv for stock...");
        try{
            pw= new PrintWriter(new File(csv_dump_path + "stock.json"));
            InputStream inputStream = new FileInputStream(csv_files_path + "stock.csv");
            InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
            CSVReader stockCsv = new CSVReader(inputStreamReader);
            Iterator<String[]> iterator = stockCsv.iterator();
            getItems();
            while(iterator.hasNext()) {
                List<String> stockRowList = new ArrayList<String>();
                String[] stockRow = iterator.next();
                stockRowList.add("\"s_w_id\" : "+stockRow[0]); //w_id
                stockRowList.add("\"s_i_id\" : "+stockRow[1]); //i_id
                //stockRowList.add("\"s_i_ref\" :ObjectId( \""+ items.get(Integer.parseInt(stockRow[1]))+"\")"); //i_id
                stockRowList.add("\"s_quantity\" : "+stockRow[2]); //s_quantity
                stockRowList.add("\"s_ytd\" : "+stockRow[3]); //s_ytd
                stockRowList.add("\"s_order_cnt\" : "+stockRow[4] +""); //s_order_cnt
                stockRowList.add("\"s_remote_cnt\" : "+stockRow[5]+""); //s_remote_cnt
                stockRowList.add("\"s_dist_01\" : \""+stockRow[6]+"\""); //dist_01
                stockRowList.add("\"s_dist_02\" : \""+stockRow[7]+"\""); //dist_02
                stockRowList.add("\"s_dist_03\" : \""+stockRow[8]+"\""); //dist_03
                stockRowList.add("\"s_dist_04\" : \""+stockRow[9]+"\""); //dist_04
                stockRowList.add("\"s_dist_05\" : \""+stockRow[10]+"\""); //dist_05
                stockRowList.add("\"s_dist_06\" : \""+stockRow[11]+"\""); //dist_06
                stockRowList.add("\"s_dist_07\" : \""+stockRow[12]+"\""); //dist_07
                stockRowList.add("\"s_dist_08\" : \""+stockRow[13]+"\""); //dist_08
                stockRowList.add("\"s_dist_09\" : \""+stockRow[14]+"\""); //dist_09
                stockRowList.add("\"s_dist_10\" : \""+stockRow[15]+"\""); //dist_10
                stockRowList.add("\"s_data\" : \""+stockRow[16]+"\""); //data
                String stock = "{"+StringUtils.join(stockRowList, ",")+"}";
                pw.write(stock+"\r\n");
                pw.flush();
            }
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("Error in  preparing stock csv");
        } finally {
            pw.close();
            logger.info("done preparing stock csv!!");
        }
    }
}