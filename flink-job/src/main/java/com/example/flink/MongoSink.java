package com.example.flink;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.bson.Document;

public class MongoSink extends RichSinkFunction<StatsResult> {

    private transient MongoClient mongoClient;
    private transient MongoCollection<Document> collection;

    private final String mongoUri;
    private final String databaseName;
    private final String collectionName;

    public MongoSink(String mongoUri, String databaseName, String collectionName) {
        this.mongoUri = mongoUri;
        this.databaseName = databaseName;
        this.collectionName = collectionName;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        MongoClientURI uri = new MongoClientURI(mongoUri);
        mongoClient = new MongoClient(uri);
        MongoDatabase database = mongoClient.getDatabase(databaseName);
        collection = database.getCollection(collectionName);
    }

    @Override
    public void invoke(StatsResult value, Context context) {
        Document document = new Document()
                .append("windowType", value.getWindowType())
                .append("count", value.getCount())
                .append("avgCheck", value.getAvgCheck())
                .append("timestamp", System.currentTimeMillis());

        try {
            collection.insertOne(document);
        } catch (com.mongodb.MongoWriteException e) {
            if (e.getCode() == 11000) {
                System.out.println("Пропускаем дубли: " + value);
            } else {
                throw e;
            }
        }
    }

    @Override
    public void close() throws Exception {
        if (mongoClient != null) {
            mongoClient.close();
        }
        super.close();
    }
}