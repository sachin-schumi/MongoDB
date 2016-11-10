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

    private static Map<String,String> orderLine = new HashMap<String,String>();

    public void getOrderLine(Properties properties,int start ,int end)
    {
        try {
            String csv_files_path = properties.getProperty("csv_files_path");
            InputStream inputStream = new FileInputStream(csv_files_path + "order-line.csv");
            InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
            CSVReader warehouseCsv = new CSVReader(inputStreamReader);
            Iterator<String[]> iterator = warehouseCsv.iterator();
            Lucene lucene = new Lucene();
            List<String> order_items = new ArrayList<String>();
            String prev_key = "1,1,1";
            String orderItemList = "";
            while(iterator.hasNext()){

                String[] row = iterator.next();
                int w_id = Integer.parseInt(row[0]);
                if(w_id < start)
                    continue;
                if(w_id > end)
                    break;
                String key = row[0] + "," + row[1] + ","+row[2];
                String line = String.join(",", row);

                if(prev_key.equals(key))
                {
                    String[] orderLineRow = line.split(",");
                    String str = "{\"i_id\": "+orderLineRow[4]+","+ "\"ol_number\": "+orderLineRow[3]+","+"\"supply_w_id\": "+orderLineRow[7]+","+
                            "\"i_amount\": "+orderLineRow[6]+","+"\"i_quantity\": "+ orderLineRow[8]+","+ "\"i_delivery_d\": "+"\""+"\""+","+"\"i_dist_info\": "+"\""+orderLineRow[9]+"\""+"}";
                    order_items.add(str);
                }
                else
                {
                    String op = "\"o_items\":["+ String.join(",",order_items) +"]}";
                    orderLine.put(prev_key,op);
                    order_items = new ArrayList<String>();
                    prev_key = key;
                }
                // orderLine.put(row[0]+","+row[1],line);
            }
        }
        catch (Exception e)
        {}
    }


    public void prepareCsv(Properties properties) {
        String csv_dump_path = properties.getProperty("csv_dump_path");
        String csv_files_path = properties.getProperty("csv_files_path");
        PrintWriter pw = null;
        //getOrderLine(properties);
        logger.info("Preparing csv for NewOrderTransaction...");
        try{
            pw = new PrintWriter(new File(csv_dump_path+"new_order_transaction.json"));
            InputStream inputStream = new FileInputStream(csv_files_path+"order.csv");
            InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
            CSVReader orderCsv = new CSVReader(inputStreamReader);
            Iterator<String[]> iterator =  orderCsv.iterator();

            int cnt = 0;
            int start = 1;
            int end = 8;
            getOrderLine(properties,start,end);
            String op = "";
            List<String> finalOutput = new ArrayList<String>();
            while(iterator.hasNext())
            {
                cnt ++;
                op = "";
                String[] orderRow = iterator.next();
                int w_id = Integer.parseInt(orderRow[0]);
                if(w_id > end) {
                    start = start + 8;
                    end = end + 8;
                    orderLine = new HashMap<String,String>();
                    getOrderLine(properties,start,end);

                }
                String id = orderRow[0]+orderRow[1]+orderRow[2];
                String formattedDate = formatDateAsIso8601(orderRow[7],"yyyy-MM-dd HH:mm:ss.S");
                op += "{\"o_w_id\":"+orderRow[0]+",\"o_d_id\":"+orderRow[1]+",\"o_id\":"+orderRow[2]+",\"c_id\":"+orderRow[3]
                        +",\"o_entry_d\":ISODate(\""+formattedDate+"\"),";
                if(!orderRow[4].equals("null") )
                    op +="\"o_carrier_id\" : "+orderRow[4]+",";
                else
                    op +="\"o_carrier_id\" : -1,";
                op +="\"o_ol_cnt\" : "+orderRow[5]+",\"o_all_local\" :"+orderRow[6]+",";

               String orderLineItems = orderLine.get(orderRow[0] + ","+orderRow[1] + ","+orderRow[2]);

                op +=orderLineItems;
                finalOutput.add(op);
                if(cnt % 1000 == 0) {
                    for(String s : finalOutput)
                    {
                        pw.write(s+"\r\n");
                    }
                    pw.flush();
                    finalOutput = new ArrayList<String>();
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
