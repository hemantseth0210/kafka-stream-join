package com.github.seth.kafkastream;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.Scanner;
import java.util.concurrent.ExecutionException;

public class UserDataProducer {

    public static void main(String[] args) throws InterruptedException, ExecutionException {

        // Create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Create Safe/Idempotent Producer
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, "3");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "1");
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");

        // Create the producer
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(properties);

        Scanner sc = new Scanner(System.in);

        System.out.println("\n Example 1 - new user \n");
        kafkaProducer.send(userRecord("john", "First=John,Last=Doe,Email=john.doe@gmail.com")).get();
        kafkaProducer.send(purchaseRecord("john", "Apples and Bananas (1)")).get();

        Thread.sleep(10000);

        System.out.println("\n Example 2 - non existing user \n");
        kafkaProducer.send(purchaseRecord("bob", "Kafka Udemy Course (2)")).get();

        Thread.sleep(10000);

        System.out.println("\n Example 3 - update user \n");
        kafkaProducer.send(userRecord("john", "First=Johnny,Last=Doe,Email=johnny.doe@gmail.com")).get();
        kafkaProducer.send(purchaseRecord("john", "Oranges (3)")).get();

        Thread.sleep(10000);

        System.out.println("\n Example 4 - non existing user then user \n");
        kafkaProducer.send(purchaseRecord("David", "Computer (4)")).get();
        kafkaProducer.send(userRecord("David", "First=David,Last=Langer,Email=david.langer@gmail.com")).get();
        kafkaProducer.send(purchaseRecord("David", "Books (4)")).get();
        kafkaProducer.send(userRecord("David", null)).get();

        Thread.sleep(10000);

        System.out.println("\n Example 5 - user then delete then data \n");
        kafkaProducer.send(userRecord("alice", "First=Alice")).get();
        kafkaProducer.send(userRecord("alice", null)).get();
        kafkaProducer.send(purchaseRecord("alice", "Apache Kafka Books (5)")).get();

        Thread.sleep(10000);

        System.out.println("End of demo");
        kafkaProducer.close();

    }

    private static ProducerRecord<String, String> purchaseRecord(String key, String value) {
        return new ProducerRecord<>("user-purchases", key, value);
    }

    private static ProducerRecord<String, String> userRecord(String key, String value) {
        return new ProducerRecord<>("user-table", key, value);
    }
}
