package com.github.sigor.kafka.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/* Useful program terminating log
[Thread-1] INFO com.github.sigor.kafka.tutorial1.ConsumerDemoWithThread - Caught shutdown hook
[Thread-0] INFO com.github.sigor.kafka.tutorial1.ConsumerDemoWithThread - Received shutdown signal!
[Thread-0] INFO com.github.sigor.kafka.tutorial1.ConsumerDemoWithThread - finally block execution
[Thread-0] INFO org.apache.kafka.clients.consumer.internals.ConsumerCoordinator - [Consumer clientId=consumer-my-sixth-application-1, groupId=my-sixth-application] Revoke previously assigned partitions first_topic-0, first_topic-1, first_topic-2
[Thread-0] INFO org.apache.kafka.clients.consumer.internals.AbstractCoordinator - [Consumer clientId=consumer-my-sixth-application-1, groupId=my-sixth-application] Member consumer-my-sixth-application-1-a0ba8122-f4ad-423e-a3ad-62c310ab8b65 sending LeaveGroup request to coordinator yxa:9092 (id: 2147483647 rack: null) due to the consumer is being closed
[main] INFO com.github.sigor.kafka.tutorial1.ConsumerDemoWithThread - Application is closing
[Thread-1] INFO com.github.sigor.kafka.tutorial1.ConsumerDemoWithThread - Application has exited
 */

public class ConsumerDemoWithThread {
    public static void main(String[] args) {
        new ConsumerDemoWithThread().run();
    }

    // hack just for ability to create object ConsumerDemoWithThread into main
    private ConsumerDemoWithThread() {

    }

    private void run(){
        Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThread.class.getName());

        String bootstrapserver = "127.0.0.1:9092";
        String group_id = "my-sixth-application";
        String topic = "first_topic";

        // latch for dealing with multiply threads
        CountDownLatch latch = new CountDownLatch(1);

        // create consumer runnable
        logger.info("Creating the consumer thread");
        Runnable myConsumerRunnable = new ConsumerRunnable(
                bootstrapserver,
                group_id,
                topic,
                latch
        );

        // start thread
        Thread myThread = new Thread(myConsumerRunnable);
        myThread.start();

        // add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread( () -> {
            logger.info("Caught shutdown hook");
            ((ConsumerRunnable) myConsumerRunnable).shutdown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            logger.info("Application has exited");
        }

        ));

        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.error("Application got interrupted", e);
        } finally {
            logger.info("Application is closing");
        }

    }

    public class ConsumerRunnable implements Runnable {
        private Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThread.class.getName());

        private CountDownLatch latch;
        private KafkaConsumer<String, String> consumer;

        public ConsumerRunnable(String bootstrapserver,
                                String group_id,
                                String topic,
                                CountDownLatch latch) {
            this.latch = latch;

            // consumer config
            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapserver);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, group_id);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

            // create consumer
            consumer = new KafkaConsumer<String, String>(properties);

            // subscribe to topics
            consumer.subscribe(Arrays.asList(topic));
        }
        @Override
        public void run() {
            // poll data
            try {
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100)); // Duration since v2
                    for (ConsumerRecord record : records) {
                        logger.info("Key: " + record.key() + ", Value: " + record.value());
                        logger.info("Partition: " + record.partition() + ", Offset: " + record.offset());
                    }
                }
            } catch (WakeupException e) {
                logger.info("Received shutdown signal!");
            } finally {
                logger.info("finally block execution");
                consumer.close();
                // tell our main code consumer exited
                latch.countDown();
            }

        }

        public void shutdown() {
            // wakeup is special method to interrupt .poll()
            // it will throw exception WakeUpException
            consumer.wakeup();


        }
    }
}
