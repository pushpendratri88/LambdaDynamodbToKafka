package com.brainstorm.handler;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.DynamodbEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.Socket;
import java.util.Properties;

public class DynamoDBStreamHandler implements RequestHandler<DynamodbEvent, String> {

    private static final Logger logger = LoggerFactory.getLogger(DynamoDBStreamHandler.class);
    private final ObjectMapper objectMapper = new ObjectMapper();
    private static final String TOPIC = "raw-events";
    private static final String BOOTSTRAP_SERVERS = "172.31.15.170:9092";

    private static KafkaProducer<String, String> kafkaProducer;

    // Static block to initialize Kafka Producer once
    static {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        kafkaProducer = new KafkaProducer<>(props);
    }

    @Override
    public String handleRequest(DynamodbEvent dynamodbEvent, Context context) {
        try (Socket socket = new Socket("13.203.157.163", 9092)) {
            logger.info("Successfully connected to Kafka broker.");
        } catch (IOException e) {
            logger.error("Failed to connect to Kafka broker.", e);
        }

        dynamodbEvent.getRecords().forEach(record -> {
            String eventName = record.getEventName();  // INSERT, MODIFY
            logger.info("EventName : {}", eventName);
            if (record.getDynamodb().getNewImage() != null) {
                if ("INSERT".equals(eventName) || "MODIFY".equals(eventName)){
                    logger.info("New Record Image: {}", record.getDynamodb().getNewImage());
                    try {
                        String jsonData = objectMapper.writeValueAsString(record.getDynamodb().getNewImage());
//                        kafkaProducer.send(new ProducerRecord<>(TOPIC, jsonData));
                        logger.info("Published event to Kafka: {}", jsonData);
                    } catch (Exception e) {
                        logger.error("Error sending data to Kafka", e);
                    }
                }
            }
        });
        return "Processed";
    }
}