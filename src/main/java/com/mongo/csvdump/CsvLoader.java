package com.mongo.csvdump;

import com.mongo.utilities.Lucene;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

/**
 * Created by sachin on 22/10/2016.
 */
public class CsvLoader {

    public static void main(String[] args){
        Properties properties =null;
        Lucene lucene = new Lucene();
        try {
            String configFilePath = System.getenv("DD_CONFIG_FILE");
            InputStream inputStream = new FileInputStream(configFilePath);
            properties = new Properties();
            properties.load(inputStream);
            lucene.initSearch(properties);
            //new NewOrderCSV().prepareCsv(lucene, properties);
            new StockCSV().prepareCsv(properties);
            //new WarehouseCSV().prepareCsv(properties);
            //new CustomerCSV().prepareCsv(properties);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
