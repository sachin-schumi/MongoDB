package com.mongo.utilities;

import com.opencsv.CSVReader;
import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

/**
 * Created by sachin on 22/10/2016.
 */
public class Lucene {

    private static Logger logger = Logger.getLogger(Lucene.class);
    private static String luceneIndexFolder;
    private static String csv_files_path;
    static IndexSearcher isearcher;
    static TotalHitCountCollector collector;


    public IndexWriter createIndex() throws Exception {
        IndexWriter indexWriter = null;

        try {
            File file = new File(luceneIndexFolder);
            Directory directory = FSDirectory.open(file.toPath());
            IndexWriterConfig config = new IndexWriterConfig(new StandardAnalyzer());
            indexWriter = new IndexWriter(directory, config);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return indexWriter;
    }

    public void addDocumentToIndex(IndexWriter indexWriter, String csvFile, String csvType, String keyType) throws Exception {
        System.out.println("Adding " + csvFile + "data to index.....");
//        ClassLoader classLoader = getClass().getClassLoader();
        InputStream inputStream = new FileInputStream(csv_files_path+csvFile);
        InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
//        InputStreamReader file = new InputStreamReader(classLoader.getResource(csvFile).openStream());
        CSVReader csv = new CSVReader(inputStreamReader);
        Iterator<String[]> iterator = csv.iterator();
        while (iterator.hasNext()) {
            String key = "";
            String[] row = iterator.next();
            Document document = new Document();
            if (csvType.equals("item-csv"))
                key = row[0];
            else if (csvType.equals("order-line-csv"))
                key = row[0] + row[1] + row[2];
            else if (csvType == "warehouse-csv")
                key = row[0];
            else if (csvType == "district-csv")
                key = row[0] + row[1];
            else if (csvType == "stock-csv")
                key = row[0] + row[1];
            else if (csvType == "customer-csv")
                key = row[0] + row[1] + row[2];
            document.add(new Field(keyType, key, TextField.TYPE_STORED));
            document.add(new Field(csvType, String.join(",", row), TextField.TYPE_STORED));
            indexWriter.addDocument(document);
        }
        System.out.println("Added " + csvFile + "data to index!!!");

    }

    public void initSearch(Properties properties) {
        try {
            luceneIndexFolder = properties.getProperty("lucene_path");
            csv_files_path = properties.getProperty("csv_files_path");
            File file = new File(luceneIndexFolder);
            Directory directory = FSDirectory.open(file.toPath());
            DirectoryReader ireader = DirectoryReader.open(directory);
            isearcher = new IndexSearcher(ireader);
            collector = new TotalHitCountCollector();
        } catch (IOException e) {

        }

    }




    public List<String> search(String searchQuery, String keyType, String csvType) throws Exception {
        List<String> items = new ArrayList<String>();
        try {
            QueryParser parser = new QueryParser(keyType, new StandardAnalyzer());
            Query query = parser.parse(searchQuery);
            isearcher.search(query, collector);
            TopDocs topDocs = isearcher.search(query, Math.max(1, collector.getTotalHits()));
            ScoreDoc[] hits = isearcher.search(query, topDocs.totalHits).scoreDocs;
            for (int i = 0; i < hits.length; i++) {
                Document hitDoc = isearcher.doc(hits[i].doc);
                items.add(hitDoc.getField(csvType).stringValue());
            }
        }
        catch (Exception e)
        {
            System.out.println(searchQuery);
            System.out.println(keyType);
        }
        return items;
    }

    public static void main(String[] args) {
        Lucene lucene = new Lucene();
        try {
            String configFilePath = System.getenv("DD_CONFIG_FILE");
            InputStream inputStream = new FileInputStream(configFilePath);
            Properties properties = new Properties();
            properties.load(inputStream);
            lucene.initSearch(properties);

            IndexWriter indexWriter = lucene.createIndex();
            lucene.addDocumentToIndex(indexWriter, "item.csv", "item-csv", "item-id");
            lucene.addDocumentToIndex(indexWriter, "order-line.csv", "order-line-csv", "order-id");
            lucene.addDocumentToIndex(indexWriter, "warehouse.csv", "warehouse-csv", "warehouse-id");
            lucene.addDocumentToIndex(indexWriter, "district.csv", "district-csv", "district-id");
            lucene.addDocumentToIndex(indexWriter, "customer.csv", "customer-csv", "customer-id");
            lucene.addDocumentToIndex(indexWriter, "stock.csv", "stock-csv", "stock-id");
            indexWriter.close();

//            lucene.search("111", "order-id", "order-line-csv");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
