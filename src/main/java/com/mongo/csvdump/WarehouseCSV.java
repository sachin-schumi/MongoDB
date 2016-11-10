package com.mongo.csvdump;

import com.mongo.utilities.*;
import com.opencsv.CSVReader;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import java.io.*;
import java.lang.reflect.Array;
import java.util.*;

/**
 * Created by sachin on 21/10/2016.
 */
public class WarehouseCSV {

    private static Logger logger = Logger.getLogger(WarehouseCSV.class);

    private static Map<String,String> districtMap = new HashMap<String,String>();

    public void getDistrict(Properties properties)
    {
        try {
            String csv_files_path = properties.getProperty("csv_files_path");
            InputStream inputStream = new FileInputStream(csv_files_path + "district.csv");
            InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
            CSVReader warehouseCsv = new CSVReader(inputStreamReader);
            Iterator<String[]> iterator = warehouseCsv.iterator();
            Lucene lucene = new Lucene();
            while(iterator.hasNext()){
                String[] row = iterator.next();
                String line = String.join(",", row);
                districtMap.put(row[0]+","+row[1],line);
            }
        }
        catch (Exception e)
        {}
    }


    public void prepareCsv(Properties properties) {
        getDistrict(properties);
        String csv_dump_path = properties.getProperty("csv_dump_path");
        String csv_files_path = properties.getProperty("csv_files_path");
        PrintWriter pw = null;
        logger.info("Preparing csv for warehouse....");
        try{
            pw= new PrintWriter(new File(csv_dump_path + "warehouse.json"));
            InputStream inputStream = new FileInputStream(csv_files_path + "warehouse.csv");
            InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
            CSVReader warehouseCsv = new CSVReader(inputStreamReader);
            Iterator<String[]> iterator = warehouseCsv.iterator();
            Lucene lucene = new Lucene();
            while(iterator.hasNext()){
                List<String> warehouseRowList = new ArrayList<String>();
                String[] warehouseRow = iterator.next();
                warehouseRowList.add("\"w_id\" : "+warehouseRow[0]); //warehouse_id
                warehouseRowList.add("\"w_name\" :\""+warehouseRow[1]+"\""); //warehouse_name
                warehouseRowList.add("\"w_street_1\" :\""+warehouseRow[2]+"\""); //street 1
                warehouseRowList.add("\"w_street_2\" :\""+warehouseRow[3]+"\""); //street 2
                warehouseRowList.add("\"w_city\" :\""+warehouseRow[4]+"\""); //city
                warehouseRowList.add("\"w_state\" :\""+warehouseRow[5]+"\""); //state
                warehouseRowList.add("\"w_zip\" :\""+warehouseRow[6]+"\""); //zip
                warehouseRowList.add("\"w_tax\" : "+warehouseRow[7]); //tax
                warehouseRowList.add("\"w_ytd\" : "+warehouseRow[8]); //tax
                String warehouse = "{"+StringUtils.join(warehouseRowList, ",");
                warehouse += ",\"district\":[";
                String districtJSON = "";
                for(int i = 1;i<=10;i++)
                {
                    String[] district = districtMap.get(warehouseRow[0]+","+i).split(",");
                    //String[] district = lucene.search(warehouseRow[0]+""+i, "district-id", "district-csv").get(0).split(",");
                    districtJSON += "{\"d_w_id\":"+district[0] + ",\"d_id\":"+district[1]+ ",\"d_name\":\""+district[2]
                            + "\",\"d_street_1\":\""+district[3]+ "\",\"d_street_2\":\""+district[4]+ "\"" +
                            ",\"d_city\":\""+district[5]+ "\",\"d_state\":\""+district[6]+"\",\"d_zip\":\""+district[7]
                            +"\",\"d_tax\":"+district[8]+",\"d_ytd\":"+district[9]+",\"d_next_o_id\":"+district[10]+"},";
                }
                districtJSON = districtJSON.substring(0,districtJSON.length() - 1)+"]";
                warehouse += districtJSON +"}";
                pw.write(warehouse+"\r\n");
                pw.flush();
            }
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("Error in preparing Warehouse csv!!");
        } finally {
            pw.close();
            logger.info("Done preparing Wrehouse csv!!");

        }
    }
}
