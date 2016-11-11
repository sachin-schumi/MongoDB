package com.mongo.csvdump;


import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

/**
 * Created by sachin on 22/10/2016.
 */
public class JSONLoader {

    public static void main(String[] args){
        Properties properties =null;
        try {
            String configFilePath = System.getenv("DD_CONFIG_FILE");
            InputStream inputStream = new FileInputStream(configFilePath);
            properties = new Properties();
            properties.load(inputStream);
            new NewOrderCSV().prepareCsv(properties);
            new StockCSV().prepareCsv(properties);
            new CustomerCSV().prepareCsv(properties);
            new ItemsCSV().prepareCsv(properties);
            new WarehouseCSV().prepareCsv(properties);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
