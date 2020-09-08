package com.github.sigor.kafka.tutorial2;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.swing.table.TableRowSorter;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class MessageProducer {
    Logger  logger = LoggerFactory.getLogger(MessageProducer.class.getName());

    String messageFile = "/tmp/messages.txt";

    public MessageProducer() {} // constructor

    public static void main(String[] args) {
        new MessageProducer().run();
    }

    private void run() {
        logger.info("Setup");
        String[] messages = null;

        // create message client
        try {
            messages = createMessages(messageFile);
        } catch (IOException e) {
            e.printStackTrace();
        }

        // create messages producer
        KafkaProducer<String, String> producer = createKafkaProducer();

        // add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(()-> {
            logger.info("Stopping application. Close producer.");
            producer.close();
            logger.info("done");
        }));

        // loop to send messages to kafka
        if (messages != null) {
            for(String msg : messages ) {

                if(msg != null) {
                    logger.info(msg);
                    producer.send(new ProducerRecord<>("messages_text", null, msg), new Callback() {
                        @Override
                        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                            if(e != null) {
                                logger.error("Something bad happened", e);
                            }
                        }
                    });
                }
            }
        }
        logger.info("End of application");
    }

    public String[] createMessages(String filename) throws IOException
    {
        FileReader fileReader = new FileReader(filename);
        Date date = new Date();
        SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy h:mm:ss a");
        String formattedDate = sdf.format(date);

        BufferedReader bufferedReader = new BufferedReader(fileReader);
        List<String> lines = new ArrayList<String>();
        String line = null;

        while ((line = bufferedReader.readLine()) != null)
        {
            lines.add("{\"date\": \""+ formattedDate + "\",\"text\": \""+line+"\"}");
        }

        bufferedReader.close();

        return lines.toArray(new String[lines.size()]);
    }

    public KafkaProducer<String, String> createKafkaProducer() {
        String bootstrapservers = "127.0.0.1:9092";
        // create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapservers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create idempotent producer
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");

        // create producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        return producer;

    }
}
