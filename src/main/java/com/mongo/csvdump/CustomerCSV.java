package com.mongo.csvdump;

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
public class CustomerCSV {

    private static Logger logger = Logger.getLogger(CustomerCSV.class);

    private String formatDateAsIso8601(final String inputDateAsString, final String inputStringFormat) throws ParseException {

        final DateFormat iso8601DateFormatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm'Z'", Locale.ENGLISH);
        iso8601DateFormatter.setTimeZone(TimeZone.getTimeZone("UTC"));

        final DateFormat inputDateFormatter = new SimpleDateFormat(inputStringFormat, Locale.ENGLISH);
        final Date inputDate = inputDateFormatter.parse(inputDateAsString);

        return iso8601DateFormatter.format(inputDate);
    }

    public void prepareCsv(Properties properties) {
        String csv_dump_path = properties.getProperty("csv_dump_path");
        String csv_files_path = properties.getProperty("csv_files_path");
        PrintWriter pw = null;
        logger.info("Preparing csv for Customer...");
        try{
            pw = new PrintWriter(new File(csv_dump_path+ "customer.json"));
            InputStream inputStream = new FileInputStream(csv_files_path + "customer.csv");
            InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
            CSVReader customerCsv = new CSVReader(inputStreamReader);
            Iterator<String[]> iterator = customerCsv.iterator();
            while(iterator.hasNext()) {
                List<String> customerRowList = new ArrayList<String>();
                String[] customerRow = iterator.next();
                customerRowList.add("\"c_w_id\" : "+customerRow[0]); // w_id
                customerRowList.add("\"c_d_id\" : "+customerRow[1]); //d_id
                customerRowList.add("\"c_id\" : "+customerRow[2]); //c_id
                customerRowList.add("\"c_first\" :\""+customerRow[3] + "\""); //c_first
                customerRowList.add("\"c_middle\" :\""+customerRow[4]+ "\""); //c_middle
                customerRowList.add("\"c_last\" : \""+customerRow[5]+ "\""); //c_last
                customerRowList.add("\"c_street_1\" :\""+customerRow[6]+ "\""); //street 1
                customerRowList.add("\"c_street_2\" :\""+customerRow[7]+ "\""); //street 2
                customerRowList.add("\"c_city\" :\""+customerRow[8]+ "\""); //city
                customerRowList.add("\"c_state\" :\""+customerRow[9]+ "\""); //state
                customerRowList.add("\"c_zip\" :\""+customerRow[10]+ "\""); //zip
                customerRowList.add("\"c_phone\" :\""+customerRow[11]+ "\""); //c_phone
                String formattedDate = formatDateAsIso8601(customerRow[12],"yyyy-MM-dd HH:mm:ss.S");
                customerRowList.add("\"c_since\" :ISODATE(\""+formattedDate+ "\")"); //since
                customerRowList.add("\"c_credit\" :\""+customerRow[13]+ "\""); //credit
                customerRowList.add("\"c_credit_lim\" :"+customerRow[14]+ ""); //credit_lim
                customerRowList.add("\"c_discount\" :"+customerRow[15]+ ""); //discount
                customerRowList.add("\"c_balance\" :"+customerRow[16]+ ""); //discount
                customerRowList.add("\"c_ytd_payment\" :"+customerRow[17]+ ""); //discount
                customerRowList.add("\"c_payment_cnt\" :"+customerRow[18]+ ""); //discount
                customerRowList.add("\"c_delivery_cnt\" :"+customerRow[19]+ ""); //discount
                customerRowList.add("\"c_data\" :\""+customerRow[20]+ "\""); //data
                String customer = StringUtils.join(customerRowList, ",");
                pw.write("{"+customer+"}"+"\r\n");
                pw.flush();
            }
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("Error in  preparing customer csv");
        } finally {
            pw.close();
            logger.info("Done preparing customer csv!!");
        }
    }
}
