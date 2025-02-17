package org.example;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class Main {
    public static void main(String[] args) throws Exception {
        System.out.println("Processing.......");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String inputTopic = "prescriptions-ack";

        FlinkKafkaConsumer<String> kafkaConsumer = Consumer.createKafkaConsumer(inputTopic);

        DataStream<String> kafkaStream = env.addSource(kafkaConsumer);

        DataStream<PrescriptionAckRecord> prescriptionAckStream = kafkaStream.map(new MapFunction<String, PrescriptionAckRecord>() {
            private final ObjectMapper objectMapper = new ObjectMapper();

            @Override
            public PrescriptionAckRecord map(String value) throws Exception {
                JsonNode payload = objectMapper.readTree(value);
                JsonNode msh = payload.path("msh");
                String hmisCode = msh.path("hmisCode").asText();
                return new PrescriptionAckRecord(hmisCode, value);
            }
        });

        Properties producerProperties = new Properties();
        producerProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "154.120.216.119:9093,102.23.123.251:9093,102.23.120.153:9093");
        producerProperties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        producerProperties.setProperty(ProducerConfig.RETRIES_CONFIG, "3");
        producerProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        producerProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer"); // Use ByteArraySerializer

        producerProperties.setProperty("security.protocol", "SASL_PLAINTEXT");
        producerProperties.setProperty("sasl.mechanism", "SCRAM-SHA-256");
        producerProperties.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required "
                + "username=\"admin\" "
                + "password=\"075F80FED7C6\";");

        producerProperties.setProperty("metadata.fetch.timeout.ms", "120000");

        prescriptionAckStream.addSink(new FlinkKafkaProducer<>(
                "default-topic",
                new KafkaSerializationSchema<PrescriptionAckRecord>() {
                    @Override
                    public ProducerRecord<byte[], byte[]> serialize(PrescriptionAckRecord record, Long timestamp) {
                        String topic = "h-" + record.hmisCode + "_m-PR";
                        return new ProducerRecord<>(topic, record.payload.getBytes()); // Send as byte[]
                    }
                },
                producerProperties,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE
        ));

        env.execute("Prescription-Ack Router Job");
    }
    public static class PrescriptionAckRecord {
        public String hmisCode;
        public String payload;

        public PrescriptionAckRecord(String hmisCode, String payload) {
            this.hmisCode = hmisCode;
            this.payload = payload;
        }
    }
}