package io.github.abhayrkhot.ledger.ingest;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.abhayrkhot.ledger.common.LedgerException;
import org.junit.jupiter.api.Test;
import org.springframework.dao.CannotAcquireLockException;
import org.springframework.dao.QueryTimeoutException;
import org.springframework.jdbc.CannotGetJdbcConnectionException;
import org.springframework.kafka.listener.ListenerExecutionFailedException;
import org.springframework.transaction.CannotCreateTransactionException;

class KafkaConfigTest {

    @Test
    void classifiesDatabaseOutagesAsInfrastructure() {
        assertThat(KafkaConfig.isInfrastructureFailure(new ListenerExecutionFailedException("x",
                new CannotCreateTransactionException("db down", new CannotGetJdbcConnectionException("timeout"))))).isTrue();
        assertThat(KafkaConfig.isInfrastructureFailure(new CannotAcquireLockException("lock timeout"))).isTrue();
        assertThat(KafkaConfig.isInfrastructureFailure(LedgerException.invalid("X", "bad"))).isFalse();
        assertThat(KafkaConfig.isInfrastructureFailure(new IllegalStateException("bug"))).isFalse();
        assertThat(KafkaConfig.isInfrastructureFailure(new QueryTimeoutException("statement timeout"))).isFalse();
    }
}
