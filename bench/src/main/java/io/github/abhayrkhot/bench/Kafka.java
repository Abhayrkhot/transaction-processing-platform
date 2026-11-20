package io.github.abhayrkhot.bench;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.Consumer;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

final class Kafka {

    static final String REQUESTS = "ledger.transfer-requests";
    static final String POSTED = "ledger.transactions.posted";

    static KafkaProducer<String, String> producer(String bootstrap) {
        Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        p.put(ProducerConfig.ACKS_CONFIG, "all");
        p.put(ProducerConfig.LINGER_MS_CONFIG, 5);
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return new KafkaProducer<>(p);
    }

    /** Sends at a fixed rate; returns the number of records sent. */
    static long sendPaced(KafkaProducer<String, String> producer, String topic, List<String[]> keyValues, int perSecond) {
        long start = System.nanoTime();
        long sent = 0;
        for (String[] kv : keyValues) {
            long due = start + sent * 1_000_000_000L / perSecond;
            long wait = due - System.nanoTime();
            if (wait > 0) {
                Api.sleep(wait / 1_000_000);
            }
            producer.send(new org.apache.kafka.clients.producer.ProducerRecord<>(topic, kv[0], kv[1]), (md, ex) -> {
                if (ex != null) {
                    System.err.println("send failed: " + ex);
                }
            });
            sent++;
            if (sent % 20_000 == 0) {
                System.out.printf("  sent %,d / %,d%n", sent, keyValues.size());
            }
        }
        producer.flush();
        return sent;
    }

    /** Sum of (end offset - committed offset) over the group's partitions of a topic. */
    static long lag(String bootstrap, String group, String topic) throws Exception {
        Properties p = new Properties();
        p.put("bootstrap.servers", bootstrap);
        try (Admin admin = Admin.create(p)) {
            Map<TopicPartition, OffsetAndMetadata> committed =
                    admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get();
            int partitions = admin.describeTopics(List.of(topic)).allTopicNames().get().get(topic).partitions().size();
            Map<TopicPartition, OffsetSpec> request = new java.util.HashMap<>();
            for (int i = 0; i < partitions; i++) {
                request.put(new TopicPartition(topic, i), OffsetSpec.latest());
            }
            Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> ends = admin.listOffsets(request).all().get();
            long lag = 0;
            for (var e : ends.entrySet()) {
                OffsetAndMetadata c = committed.get(e.getKey());
                lag += e.getValue().offset() - (c == null ? 0 : c.offset());
            }
            return lag;
        }
    }

    /** Reads a whole topic from the beginning to its current end. */
    static void readAll(String bootstrap, String topic, Consumer<ConsumerRecord<String, String>> sink) {
        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(p)) {
            List<TopicPartition> parts = consumer.partitionsFor(topic).stream()
                    .map(i -> new TopicPartition(topic, i.partition())).toList();
            consumer.assign(parts);
            consumer.seekToBeginning(parts);
            Map<TopicPartition, Long> end = consumer.endOffsets(parts);
            while (parts.stream().anyMatch(tp -> consumer.position(tp) < end.get(tp))) {
                consumer.poll(Duration.ofMillis(500)).forEach(sink);
            }
        }
    }
}
