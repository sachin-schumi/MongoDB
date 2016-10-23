package com.mongo;

import com.mongo.transactions.LocalCache;
import com.mongo.transactions.Props;
import com.mongodb.client.MongoDatabase;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.PropertyConfigurator;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.PrintWriter;
import java.util.Properties;

/**
 * Created by Sachin on 10/22/2016.
 */
public class TransactionClient {
    public static void main(String args[])
    {
        try {
            Properties properties = Props.loadProps();
            MongoDatabase sesion = MongoSession.getSession(properties);
            new LocalCache().loadIntoCache();

            Properties props = new Properties();
            String configFilePath = System.getenv("DD_LOG_FILE");
            InputStream inputStream = new FileInputStream(configFilePath);
            props.load(inputStream);
            PropertyConfigurator.configure(props);
            PrintWriter printWriter = new PrintWriter(new File(properties.getProperty("output_path")));

            TransactionDriver t = new TransactionDriver(sesion, null, printWriter, null, null, 1, properties.getProperty("transactions_dir"));
            t.readTransactionFiles(null, printWriter);
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }
}
