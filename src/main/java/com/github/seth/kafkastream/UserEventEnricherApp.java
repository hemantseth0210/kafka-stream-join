package com.github.seth.kafkastream;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class UserEventEnricherApp {

    public static void main(String[] args) {

        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "user-event-enricher-app");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();

        GlobalKTable<String, String> userGlobalTable = builder.globalTable("user-table");

        KStream<String, String> userPurchasesStream = builder.stream("user-purchases");

        // Enrich the user purchase stream using join
        KStream<String, String> userPurchaseEnrichedJoin = userPurchasesStream
                .join(
                        userGlobalTable,
                        (key, value) -> key, // map from the (key,value) of this stream to the key of GlobalKTable
                        (userPurchase, userInfo) -> "Purchase = " + userPurchase + ", UserInfo = [" + userInfo + "]"
                );

        userPurchaseEnrichedJoin.to("user-purchases-enriched-inner-join");

        // Enrich the user purchase stream using left join
        KStream<String, String> userPurchaseEnrichedLeftJoin = userPurchasesStream
                .leftJoin(
                        userGlobalTable,
                        (key, value) -> key, // map from the (key,value) of this stream to the key of GlobalKTable
                        (userPurchase, userInfo) -> {
                            // as this is a left join, userInfo can be null
                            if (userInfo != null) {
                                return "Purchase = " + userPurchase + ", UserInfo = [" + userInfo + "]";
                            } else {
                                return "Purchase = " + userPurchase + ", UserInfo = null";
                            }
                        });
        userPurchaseEnrichedLeftJoin.to("user-purchases-enriched-left-join");

        KafkaStreams streams = new KafkaStreams(builder.build(), properties);
        streams.cleanUp();
        streams.start();

        // printed topology
        System.out.println(streams.toString());

        // shutdown hook to correctly close the streams application
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
