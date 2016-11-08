package com.mongo;

import com.mongo.transactions.LocalCache;
import com.mongodb.client.MongoDatabase;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import java.io.*;
import java.util.Properties;

/**
 * Created by Sachin on 10/22/2016.
 */
public class TransactionClient {
    private static Logger logger = Logger.getLogger(TransactionDriver.class);
    static  int clientCount = 2;
    static long startTime = 0;
    static Properties properties;
    public static void main(String args[])
    {
        try {
            Properties properties = loadProperties();
            MongoDatabase sesion = MongoSession.getSession(properties);
            new LocalCache().loadIntoCache(properties);

            Properties props = new Properties();
            String configFilePath = System.getenv("DD_LOG_FILE");
            InputStream inputStream = new FileInputStream(configFilePath);
            props.load(inputStream);
            PropertyConfigurator.configure(props);
            PrintWriter printWriter = new PrintWriter(new File(properties.getProperty("output_path")));
            System.out.println("Enter the number of clients:");
            try {
                BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
                clientCount = Integer.parseInt(br.readLine());
            }catch (NumberFormatException e)
            {
                System.out.println("Please enter a number for the number of clients....");
            }
            startTime = System.currentTimeMillis();
            for(int i=0;i<clientCount;i++)
            {
                TransactionDriver t = new TransactionDriver(sesion,printWriter, "thread_"+i, i, properties.getProperty("transactions_dir"));
                t.start();
            }
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }


    public static Properties loadProperties()
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
