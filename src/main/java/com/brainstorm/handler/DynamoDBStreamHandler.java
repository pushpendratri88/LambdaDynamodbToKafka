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
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Properties;


public class DynamoDBStreamHandler implements RequestHandler<DynamodbEvent, String> {

    private static final Logger logger = LoggerFactory.getLogger(DynamoDBStreamHandler.class);
    private final ObjectMapper objectMapper = new ObjectMapper();
    private static final String TOPIC = "raw-events";
    private static final String BOOTSTRAP_SERVERS = "13.203.157.163:9092";

    private static KafkaProducer<String, String> kafkaProducer;

    // Static block to initialize Kafka Producer once
    static {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "lambda-producer");

        kafkaProducer = new KafkaProducer<>(props);
    }

    @Override
    public String handleRequest(DynamodbEvent dynamodbEvent, Context context) {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress("13.203.157.163", 9092), 2000);
            logger.info("Kafka connection successful!");
        } catch (Exception e) {
            logger.error("Kafka connection failed!", e);
        }

        dynamodbEvent.getRecords().forEach(record -> {
            String eventName = record.getEventName();  // INSERT, MODIFY
            logger.info("EventName : {}", eventName);
            if (record.getDynamodb().getNewImage() != null && ("INSERT".equals(eventName) || "MODIFY".equals(eventName))) {
                logger.info("New Record Image: {}", record.getDynamodb().getNewImage());
                try {
                    String jsonData = objectMapper.writeValueAsString(record.getDynamodb().getNewImage().get("logevent").getS());
                    String unescapedJson = jsonData.replace("\\", "");
                    unescapedJson = unescapedJson.substring(1, unescapedJson.length() - 1);
                    logger.info("Before publishing event to Kafka: {}", unescapedJson);
                    kafkaProducer.send(new ProducerRecord<>(TOPIC, unescapedJson)).get();
                    kafkaProducer.flush();
                    logger.info("Published event to Kafka: {}", unescapedJson);
                } catch (Exception e) {
                    logger.error("Error sending data to Kafka", e);
                }
            }
        });
        return "Processed";
    }
}