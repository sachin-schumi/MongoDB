package com.mongo.csvdump;

import com.mongo.utilities.Lucene;
import com.opencsv.CSVReader;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import java.io.*;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by sachin on 21/10/2016.
 */
public class NewOrderCSV {

    private static Logger logger = Logger.getLogger(NewOrderCSV.class);
    private String formatDateAsIso8601(final String inputDateAsString, final String inputStringFormat) throws ParseException {

        final DateFormat iso8601DateFormatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm'Z'", Locale.ENGLISH);
        iso8601DateFormatter.setTimeZone(TimeZone.getTimeZone("UTC"));

        final DateFormat inputDateFormatter = new SimpleDateFormat(inputStringFormat, Locale.ENGLISH);
        final Date inputDate = inputDateFormatter.parse(inputDateAsString);

        return iso8601DateFormatter.format(inputDate);
    }

    public void prepareCsv(Lucene lucene, Properties properties) {
        String csv_dump_path = properties.getProperty("csv_dump_path");
        String csv_files_path = properties.getProperty("csv_files_path");
        PrintWriter pw = null;
        logger.info("Preparing csv for NewOrderTransaction...");
        try{
            pw = new PrintWriter(new File(csv_dump_path+"new_order_transaction1.json"));
            InputStream inputStream = new FileInputStream(csv_files_path+"order.csv");
            InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
            CSVReader orderCsv = new CSVReader(inputStreamReader);
            Iterator<String[]> iterator =  orderCsv.iterator();
            String op = "";
            int cnt = 0;
            while(iterator.hasNext())
            {
                cnt ++;
                String[] orderRow = iterator.next();
                String id = orderRow[0]+orderRow[1]+orderRow[2];
                String formattedDate = formatDateAsIso8601(orderRow[7],"yyyy-MM-dd HH:mm:ss.S");
                op += "{\"o_w_id\":"+orderRow[0]+",\"o_d_id\":"+orderRow[1]+",\"o_id\":"+orderRow[2]+",\"c_id\":"+orderRow[3]
                        +",\"o_entry_d\":ISODate(\""+formattedDate+"\"),";
                if(!orderRow[4].equals("null") )
                    op +="\"o_carrier_id\" : "+orderRow[4]+",";
                else
                    op +="\"o_carrier_id\" : -1,";
                op +="\"o_ol_cnt\" : "+orderRow[5]+",\"o_all_local\" :"+orderRow[6]+",\"o_items\":[";
                List<String> orderLineItems = lucene.search(orderRow[0] + orderRow[1] + orderRow[2], "order-id", "order-line-csv");
                String orderItemList = "";
                for(String string: orderLineItems){
                    String[] orderLineRow = string.split(",");
                    String str = "{\"i_id\": "+orderLineRow[4]+","+ "\"ol_number\": "+orderLineRow[3]+","+"\"supply_w_id\": "+orderLineRow[7]+","+
                            "\"i_amount\": "+orderLineRow[6]+","+"\"i_quantity\": "+ orderLineRow[8]+","+ "\"i_delivery_d\": "+"\""+"\""+","+"\"i_dist_info\": "+"\""+orderLineRow[9]+"\""+"}";
                    orderItemList += str+",";
                }
                String itemSet = orderItemList.substring(0,orderItemList.length() -1);
                op += itemSet + "]}" + "\r\n";
                if(cnt % 1000 == 0) {
                    System.out.println(cnt);
                    pw.write(op);
                    pw.flush();
                    op = "";
                }
        }
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("Error in  preparing NewOrderTransaction csv!!");
        } finally {
            pw.close();
            logger.info("Done preparing NewOrderTransaction csv!!");
        }
    }
}
