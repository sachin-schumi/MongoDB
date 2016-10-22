package com.mongo.transactions;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

/**
 * Created by Sachin on 10/22/2016.
 */
public class Props {

    public  static Properties properties;
    public static Properties loadProps()
    {
        try {
            if(properties == null) {
                properties = new Properties();
                String configFilePath = System.getenv("DD_CONFIG_FILE");
                //export DD_CONFIG_FILE="/Users/ritesh/Documents/projects/nus/config.properties"
                InputStream inputStream = new FileInputStream(configFilePath);
                properties.load(inputStream);
            }
        }
        catch (Exception e)
        {}
        return properties;
    }
}
