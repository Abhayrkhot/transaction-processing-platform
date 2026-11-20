package io.github.abhayrkhot.ledger.ingest;

import io.github.abhayrkhot.ledger.common.LedgerException;
import io.github.abhayrkhot.ledger.common.LedgerProperties;
import io.micrometer.core.instrument.MeterRegistry;
import java.sql.SQLRecoverableException;
import java.sql.SQLTransientException;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.QueryTimeoutException;
import org.springframework.dao.TransientDataAccessException;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.RetryListener;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;
import org.springframework.transaction.CannotCreateTransactionException;
import org.springframework.transaction.TransactionTimedOutException;
import org.springframework.util.backoff.ExponentialBackOff;

@Configuration
class KafkaConfig {

    private static final Logger log = LoggerFactory.getLogger(KafkaConfig.class);

    static final String DLT_SUFFIX = ".dlt";

    @Bean
    NewTopic transferRequestsTopic(LedgerProperties properties) {
        return topic(properties, properties.kafka().transferRequestsTopic());
    }

    @Bean
    NewTopic transferRequestsDeadLetterTopic(LedgerProperties properties) {
        return topic(properties, properties.kafka().transferRequestsTopic() + DLT_SUFFIX);
    }

    @Bean
    NewTopic postedTopic(LedgerProperties properties) {
        return topic(properties, properties.kafka().postedTopic());
    }

    @Bean
    NewTopic postedDeadLetterTopic(LedgerProperties properties) {
        return topic(properties, properties.kafka().postedTopic() + DLT_SUFFIX);
    }

    /**
     * Retry policy for every listener. Retries are blocking, so a partition keeps its order
     * while a record is retried.
     * <ul>
     *   <li>Invalid messages ({@link LedgerException}, unparseable JSON): dead-letter at once.</li>
     *   <li>Database unavailable, deadlock or serialization failure: retry with exponential back-off
     *       capped at 10 s and no attempt limit. Sending these to the DLT would empty the topic
     *       into the DLT during an outage.</li>
     *   <li>Anything else, including statement/transaction timeouts: 4 retries, then dead-letter, so
     *       one record that can never finish cannot stall its partition.</li>
     * </ul>
     */
    @Bean
    DefaultErrorHandler kafkaErrorHandler(KafkaTemplate<String, String> template, MeterRegistry meters) {
        var recoverer = new DeadLetterPublishingRecoverer(template,
                (record, ex) -> new TopicPartition(record.topic() + DLT_SUFFIX, -1));

        var bounded = new ExponentialBackOffWithMaxRetries(4);
        bounded.setInitialInterval(500);
        bounded.setMultiplier(2.0);
        bounded.setMaxInterval(5_000);

        var handler = new DefaultErrorHandler(recoverer, bounded);
        handler.setBackOffFunction((record, ex) -> isInfrastructureFailure(ex) ? unbounded() : null);
        handler.addNotRetryableExceptions(LedgerException.class);
        handler.setRetryListeners(new RetryListener() {
            @Override
            public void failedDelivery(ConsumerRecord<?, ?> record, Exception ex, int attempt) {
                meters.counter("ledger.kafka.retries", "topic", record.topic()).increment();
                log.warn("delivery attempt {} failed for {}-{}@{}: {}", attempt, record.topic(), record.partition(),
                        record.offset(), rootCause(ex));
            }

            @Override
            public void recovered(ConsumerRecord<?, ?> record, Exception ex) {
                meters.counter("ledger.kafka.dead_lettered", "topic", record.topic()).increment();
                log.warn("dead-lettered {}-{}@{}: {}", record.topic(), record.partition(), record.offset(), rootCause(ex));
            }
        });
        return handler;
    }

    static boolean isInfrastructureFailure(Throwable ex) {
        for (Throwable t = ex; t != null; t = t.getCause()) {
            if (t instanceof QueryTimeoutException || t instanceof TransactionTimedOutException) {
                // Could be this record's own cost (e.g. a huge history scan): bounded retries.
                return false;
            }
            if (t instanceof TransientDataAccessException
                    || t instanceof DataAccessResourceFailureException
                    || t instanceof CannotCreateTransactionException
                    || t instanceof SQLTransientException
                    || t instanceof SQLRecoverableException) {
                return true;
            }
        }
        return false;
    }

    private static String rootCause(Throwable ex) {
        Throwable t = ex;
        while (t.getCause() != null && t.getCause() != t) {
            t = t.getCause();
        }
        return t.toString();
    }

    private static ExponentialBackOff unbounded() {
        var backOff = new ExponentialBackOff(200, 2.0);
        backOff.setMaxInterval(10_000);
        return backOff;
    }

    private static NewTopic topic(LedgerProperties properties, String name) {
        return TopicBuilder.name(name)
                .partitions(properties.kafka().partitions())
                .replicas(properties.kafka().replicas())
                .build();
    }
}
