package com.mongo;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;
import org.apache.log4j.Logger;

import java.util.Properties;

/**
 * Created by sachin on 22/10/16.
 */
public final class MongoSession {
    private static MongoDatabase session = null;
    private static Logger logger = Logger.getLogger(MongoSession.class);
    private MongoSession() {
    }

    private static void createSession(Properties properties) throws Exception {
        MongoClient mongo = new MongoClient(properties.getProperty("mongo_ip"), 27017);
        session = mongo.getDatabase(properties.getProperty("db_name"));
        logger.info("Session connected to cluster " +session.getName());
    }

    public static MongoDatabase getSession(Properties properties) {
        if (session == null) {
            try {
                createSession(properties);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return session;
    }

    public static void closeSession() {
        if (session != null) {
            session.drop();
            logger.info("Session closed to " + session.getName());
        }
    }
}
