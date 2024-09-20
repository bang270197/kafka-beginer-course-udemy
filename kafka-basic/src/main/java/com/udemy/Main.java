package com.udemy;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class Main {
    public static final Logger logger = LoggerFactory.getLogger(Main.class.getName());

    public static void main(String[] args) {
        Properties properties =new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");

        Properties props = new Properties();
        props.put("bootstrap.servers", "127.0.0.1:9092");  // Kafka broker
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // Thêm cấu hình SASL
//        props.setProperty("security.protocol", "SASL_SSL");  // Sử dụng SASL_PLAINTEXT (hoặc SASL_SSL nếu bạn kết hợp với SSL)
//        props.setProperty("sasl.mechanism", "PLAIN");
//        props.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"bangnn\" password=\"Aa@123\";");


        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        producer.send(new ProducerRecord<>("TOPIC-2",  "123"));

        producer.flush();

        producer.close();

    }
}