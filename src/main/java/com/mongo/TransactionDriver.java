package com.mongo;

import com.mongo.transactions.*;
import com.mongodb.client.MongoDatabase;
import org.apache.log4j.Logger;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import static com.mongo.TransactionClient.startTime;
import static java.lang.String.*;

public class TransactionDriver implements Runnable {
    private static Logger logger = Logger.getLogger(TransactionDriver.class);
    private MongoDatabase session;
    private PrintWriter printWriter;
    private String threadName;
    private long startTimeOfTransaction;
    private int currentThread;
    private String transactionDir;
    private static int noOfClientsDone = 0;
    static int totalNumberOfTransactionsProcessed = 0;
    public TransactionDriver(){}

    public TransactionDriver(MongoDatabase session, PrintWriter printWriter, String threadName,int currentThread, String transactionDir) {
        this.session = session;
        this.printWriter = printWriter;
        this.threadName = threadName;
        this.startTimeOfTransaction = System.currentTimeMillis();
        this.currentThread = currentThread;
        this.transactionDir = transactionDir;
    }

    public void start()
    {
        try {
            Thread t = new Thread(this, threadName);
            t.start();
        }
        catch (Exception e)
        {}
    }

    public void run() {
        int noOfTransactionsExecuted = readTransactionFiles(printWriter);
        logger.info("["+threadName+"]Ended executing transactions for the thread ::" + threadName + ", Total transactions = " + noOfTransactionsExecuted + " for the file " + currentThread + ".txt");
        totalNumberOfTransactionsProcessed += noOfTransactionsExecuted;
        noOfClientsDone ++;
        if(noOfClientsDone == TransactionClient.clientCount)
        {

            long timeInMs = System.currentTimeMillis() - startTime;
            String diff = format("%02dmin%02dsec", TimeUnit.MILLISECONDS.toMinutes(timeInMs),
                    TimeUnit.MILLISECONDS.toSeconds(timeInMs) - TimeUnit.MINUTES.toSeconds(TimeUnit.MILLISECONDS.toMinutes(timeInMs))
            );
            logger.info("[Total Number of transactions processed: " + totalNumberOfTransactionsProcessed);
            double transactionsPerSecond = (double) totalNumberOfTransactionsProcessed / TimeUnit.MILLISECONDS.toSeconds(timeInMs);
            logger.info("Total elapsed time for processing the transactions (in seconds) : " + TimeUnit.MILLISECONDS.toSeconds(timeInMs) + " seconds");
            logger.info("Transaction throughput (number of transactions processed per second): " + transactionsPerSecond);
        }
    }

    public int readTransactionFiles(PrintWriter printWriter) {
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
        StockLevelTransaction stockLevelTransaction = new StockLevelTransaction(session);
        PopularItemTransaction popularItemTransaction = new PopularItemTransaction(session);
        TopBalanceTransaction topBalanceTransaction = new TopBalanceTransaction(session);
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
                if(tranType.equals("N"))
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
                else if(tranType.equals("P"))
                {
                    ++t2cnt;
                    w_id = Integer.parseInt(content[1]);
                    d_id = Integer.parseInt(content[2]);
                    c_id = Integer.parseInt(content[3]);
                    payment = Double.parseDouble(content[4]);
                    paymentTransaction.setPayment(w_id, d_id, c_id, payment,printWriter);
                    ++noOfTransactionsExecuted;
                }
                else if (tranType.equals("O"))
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
                else if (tranType.equals("S"))
                {
                    ++t5cnt;
                    w_id = Integer.parseInt(content[1]);
                    d_id = Integer.parseInt(content[2]);
                    threshold = Integer.parseInt(content[3]);
                    lastLOrders = Integer.parseInt(content[4]);
                    stockLevelTransaction.checkStockThreshold(w_id, d_id, threshold, lastLOrders,printWriter);
                    ++noOfTransactionsExecuted;
                }
                else if (tranType.equals("I"))
                {
                    ++t6cnt;
                    w_id = Integer.parseInt(content[1]);
                    d_id = Integer.parseInt(content[2]);
                    lastLOrders = Integer.parseInt(content[3]);
                    popularItemTransaction.checkPopularItem(w_id, d_id, lastLOrders,printWriter);
                    ++noOfTransactionsExecuted;
                }
                else if(tranType.equals("T"))
                {
                    ++t7cnt;
                    topBalanceTransaction.getTopBalance(printWriter);
                    ++noOfTransactionsExecuted;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("Failure in executing transaction for thread: " + threadName);
        }
        return noOfTransactionsExecuted;
    }
}
