package com.github.sigor.kafka.tutorial3;

import com.fasterxml.jackson.core.JsonFactory;
import com.google.gson.JsonParser;
import jdk.nashorn.internal.parser.JSONParser;
import org.apache.http.HttpHost;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ElasticSearchConsumer {

    public static KafkaConsumer<String, String> createConsumer(String topic) {

        String bootstrapserver = "127.0.0.1:9092";
        String groupId = "my-demo-elasticsearch";

        // consumer config
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapserver);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "10");

        // create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        // subscribe to topics
        consumer.subscribe(Arrays.asList(topic));

        return consumer;

    }

    public static RestHighLevelClient createClient() {
        //String hostname = "", username = "", password = "";
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("localhost", 9200, "http")
                        //new HttpHost("localhost", 9201, "http")
                )
        );
        return client;
    }

    //private static JsonParser jsonParser = new JsonParser();

    public static String extractIdFromMessage(String message) {
        // gson library
        //return jsonParser.parse(message)
        return JsonParser.parseString(message)
                .getAsJsonObject()
                .get("date")/// date as id hmm..
                .getAsString();
    }

    public static void main(String[] args) throws IOException {

        Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());

        RestHighLevelClient client = createClient();


        KafkaConsumer<String, String> consumer = createConsumer("messages_text");

        // poll data
        while(true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100)); // Duration since v2
            logger.info("Received "+ records.count() + " records");

            //BulkRequest bulkRequest = new BulkRequest();

            for(ConsumerRecord record : records) {
                logger.info((String) record.value());

                // make kafka id for idempotent consumer
                // kafka method
                String id = record.topic() + "_" + record.partition() + "_" + record.offset();

                // id from message method
                //String id = extractIdFromMessage((String) record.value());

                IndexRequest indexRequest = new IndexRequest("messages")
                        .id(id)
                        .source((String)record.value(), XContentType.JSON);

                // insert data into elasticsearch
                IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
                // String id = indexResponse.getId();
                logger.info(indexResponse.getId());
                try {
                    Thread.sleep(100);     // small delay
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            logger.info("Commiting offset...");
            consumer.commitSync();
            logger.info(("Offset has been committed"));
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        //client.close();
    }
}
