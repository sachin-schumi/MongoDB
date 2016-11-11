package com.mongo.csvdump;

import com.opencsv.CSVReader;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

/**
 * Created by sachin on 21/10/2016.
 */
public class ItemsCSV {
    private static Logger logger = Logger.getLogger(ItemsCSV.class);

    public void prepareCsv(Properties properties) {
        String csv_dump_path = properties.getProperty("csv_dump_path");
        String csv_files_path = properties.getProperty("csv_files_path");
        PrintWriter pw = null;
        logger.info("Preparing csv for stock...");
        try{
            pw= new PrintWriter(new File(csv_dump_path + "items.json"));
            InputStream inputStream = new FileInputStream(csv_files_path + "item.csv");
            InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
            CSVReader stockCsv = new CSVReader(inputStreamReader);
            Iterator<String[]> iterator = stockCsv.iterator();
            while(iterator.hasNext()) {
                List<String> stockRowList = new ArrayList<String>();
                String[] itemRow = iterator.next();
                stockRowList.add("\"i_id\":"+itemRow[0]); //i_id
                stockRowList.add("\"i_name\":\""+itemRow[1]+"\""); //i_name
                stockRowList.add("\"i_im_id\":"+itemRow[3]); //img_id
                stockRowList.add("\"i_price\":"+itemRow[2]); //i_price
                stockRowList.add("\"i_data\":\""+itemRow[4]+"\""); //i_data
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