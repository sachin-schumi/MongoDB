package com.mongo;

import com.mongo.transactions.DeliveryTransaction;
import com.mongo.transactions.NewOrderTransaction;
import com.mongo.transactions.OrderStatusTransaction;
import com.mongo.transactions.PaymentTransaction;
import com.mongo.utilities.Lucene;
import com.mongodb.Mongo;
import com.mongodb.client.MongoDatabase;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import static java.lang.String.*;

public class TransactionDriver {
    private static Logger logger = Logger.getLogger(TransactionDriver.class);
    private MongoDatabase session;
    private Lucene lucene;
    private PrintWriter printWriter;
    private String threadName;
    private long startTimeOfTransaction;
    private int currentThread;
    private String transactionDir;

    public TransactionDriver(){}

    public TransactionDriver(MongoDatabase session, Lucene lucene, PrintWriter printWriter, String threadName, Date startTime, int currentThread, String transactionDir) {
        this.session = session;
        this.lucene = lucene;
        this.printWriter = printWriter;
        this.threadName = threadName;
        this.startTimeOfTransaction = System.currentTimeMillis();
        this.currentThread = currentThread;
        this.transactionDir = transactionDir;
    }

   // @Override
    /*
    public void run() {
        int noOfTransactionsExecuted = readTransactionFiles(lucene, printWriter);
        logger.info("["+threadName+"]Ended executing transactions for the thread ::" + threadName + " total transactions=" + noOfTransactionsExecuted + " with file " + currentThread + ".txt");
        long transactionTimeInMillis = System.currentTimeMillis() - startTimeOfTransaction;
        TransactionClient.totalNumberOfTransactionsProcessed += noOfTransactionsExecuted;
        String diff = format("%02dmin%02dsec", TimeUnit.MILLISECONDS.toMinutes(transactionTimeInMillis),
                TimeUnit.MILLISECONDS.toSeconds(transactionTimeInMillis) - TimeUnit.MINUTES.toSeconds(TimeUnit.MILLISECONDS.toMinutes(transactionTimeInMillis))
        );
        logger.info("["+threadName+"]Total Number of transactions processed: " + TransactionClient.totalNumberOfTransactionsProcessed + " in "+ diff);
        double transactionsPerSecond = (double) TransactionClient.totalNumberOfTransactionsProcessed / TimeUnit.MILLISECONDS.toSeconds(transactionTimeInMillis);
        logger.info("["+threadName+"]Transaction throughput (number of transactions processed per second)::" + transactionsPerSecond);
        session.close();
        System.exit(0);
    }*/

    public int readTransactionFiles(Lucene lucene, PrintWriter printWriter) {
        /*
        logger.info("["+threadName+"]Started executing transactions for file " + currentThread + ".txt");

        */
        long startInMs = System.currentTimeMillis();
        int noOfTransactionsExecuted = 0;
        String line = "";
        int cnt = 0, t1cnt = 0, t2cnt = 0, t3cnt = 0, t4cnt = 0, t5cnt = 0, t6cnt = 0, t7cnt = 0, t8cnt = 0;
        String fname = currentThread + ".txt";
        int mod = 100;

        NewOrderTransaction newOrderTransaction = new NewOrderTransaction(session);
        PaymentTransaction paymentTransaction = new PaymentTransaction(session);
        OrderStatusTransaction orderStatusTransaction = new OrderStatusTransaction(session);
        DeliveryTransaction deliveryTransaction = new DeliveryTransaction(session);
        try {
            BufferedReader br = new BufferedReader(new FileReader(transactionDir + fname));
            while ((line = br.readLine()) != null) {
                if (cnt % mod == 0) {
                    long millis = System.currentTimeMillis() - startInMs;
                  String diff = format("%02dmin%02dsec", TimeUnit.MILLISECONDS.toMinutes(millis),
                    TimeUnit.MILLISECONDS.toSeconds(millis) - TimeUnit.MINUTES.toSeconds(TimeUnit.MILLISECONDS.toMinutes(millis))
                    );
                    logger.info("["+threadName+"]timediff-" + diff + ",total-" + cnt + ",N-" + t1cnt + ",P-" + t2cnt + ",D-" + t3cnt + ",O-" + t4cnt + ",S-" + t5cnt + ",I-" + t6cnt + ",T-" + t7cnt + ",unknown-" + t8cnt + ",transactions-" + noOfTransactionsExecuted);
                }
                String[] content = line.split(",");
                String tranType = content[0];
                //char trantype = '0';
                int w_id = 0;
                int d_id = 0;
                int c_id = 0;
                double payment = 0;
                int carrier_id = 0;
                double threshold = 0.0;
                int lastLOrders = 0;
                ++cnt;
                if(tranType.equals("NNN"))
                {
                    ++t1cnt;
                    c_id = Integer.parseInt(content[1]);
                    w_id = Integer.parseInt(content[2]);
                    d_id = Integer.parseInt(content[3]);
                    int m = Integer.parseInt(content[4]);
                    ArrayList<String> itemlineinfo = new ArrayList<String>();
                    // read m line
                    while (m > 0) {
                        --m;
                        String itemline = br.readLine();
                        if (!(itemline == null))
                            itemlineinfo.add(itemline);
                    }
                    newOrderTransaction.newOrderTransaction(w_id, d_id, c_id, itemlineinfo,session,printWriter);
                    ++noOfTransactionsExecuted;
                }
                else if(tranType.equals("PPP"))
                {
                    ++t2cnt;
                    w_id = Integer.parseInt(content[1]);
                    d_id = Integer.parseInt(content[2]);
                    c_id = Integer.parseInt(content[3]);
                    payment = Double.parseDouble(content[4]);
                    paymentTransaction.setPayment(w_id, d_id, c_id, payment,printWriter);
                    ++noOfTransactionsExecuted;
                }
                else if (tranType.equals("OO"))
                {
                    ++t4cnt;
                    w_id = Integer.parseInt(content[1]);
                    d_id = Integer.parseInt(content[2]);
                    c_id = Integer.parseInt(content[3]);
                    orderStatusTransaction.readOrderStatus(w_id, d_id, c_id,printWriter);
                    ++noOfTransactionsExecuted;
                }
                else if (tranType.equals("D"))
                {
                    ++t3cnt;
                    w_id = Integer.parseInt(content[1]);
                    carrier_id = Integer.parseInt(content[2]);
                    deliveryTransaction.readDeliveryTransaction(w_id, carrier_id,printWriter);
                    noOfTransactionsExecuted++;
                }
                /*
                switch (trantype) {
                    case 'N':
                        ++t1cnt;
                        c_id = Integer.parseInt(content[1]);
                        w_id = Integer.parseInt(content[2]);
                        d_id = Integer.parseInt(content[3]);
                        int m = Integer.parseInt(content[4]);
                        ArrayList<String> itemlineinfo = new ArrayList<String>();
                        // read m line
                        while (m > 0) {
                            --m;
                            String itemline = br.readLine();
                            if (!(itemline == null))
                                itemlineinfo.add(itemline);
                        }
                       // new NewOrderTransaction().newOrderTransaction(w_id, d_id, c_id, itemlineinfo, session, lucene, printWriter);
                        ++noOfTransactionsExecuted;
                        break;

                    case 'P':
                        ++t2cnt;
                        w_id = Integer.parseInt(content[1]);
                        d_id = Integer.parseInt(content[2]);
                        c_id = Integer.parseInt(content[3]);
                        payment = Double.parseDouble(content[4]);
                        new PaymentTransaction().setPayment(w_id, d_id, c_id, payment, session,printWriter);
                        ++noOfTransactionsExecuted;
                        break;

                    case "D":
                        ++t3cnt;
                        w_id = Integer.parseInt(content[1]);
                        carrier_id = Integer.parseInt(content[2]);
                        new DeliveryTransaction().readDeliveryTransaction(w_id, carrier_id, session, printWriter);
                        noOfTransactionsExecuted++;
                        break;
                    case "O":
                        ++t4cnt;
                        w_id = Integer.parseInt(content[1]);
                        d_id = Integer.parseInt(content[2]);
                        c_id = Integer.parseInt(content[3]);
                        new OrderStatusTransaction().readOrderStatus(w_id, d_id, c_id, session, lucene, printWriter);
                        ++noOfTransactionsExecuted;
                        break;
                    case "S":
                        ++t5cnt;
                        w_id = Integer.parseInt(content[1]);
                        d_id = Integer.parseInt(content[2]);
                        threshold = Integer.parseInt(content[3]);
                        lastLOrders = Integer.parseInt(content[4]);
                        new StockLevelTransaction().checkStockThreshold(w_id, d_id, threshold, lastLOrders, session, lucene, printWriter);
                        ++noOfTransactionsExecuted;
                        break;
                    case "I":
                        ++t6cnt;
                        w_id = Integer.parseInt(content[1]);
                        d_id = Integer.parseInt(content[2]);
                        lastLOrders = Integer.parseInt(content[3]);
                        new PopularItemTransaction().checkPopularItem(w_id, d_id, lastLOrders, session, printWriter, lucene);
                        ++noOfTransactionsExecuted;
                        break;
                    case "T":
                        ++t7cnt;
                        new TopBalanceTransaction().getTopBalance(session, printWriter,lucene);
                        ++noOfTransactionsExecuted;
                        break;
                    default:
                        ++t8cnt;
                        logger.info("unknown transaction type " + tranType + " in count " + noOfTransactionsExecuted + " for thread " + this.threadName);
                    */
                }
            //}
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("Failure in executing transaction for thread: " + threadName);
        }
        return noOfTransactionsExecuted;
    }
}
