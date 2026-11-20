package io.github.abhayrkhot.ledger.transfer;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class TransferCommandTest {

    private final UUID a = UUID.randomUUID();
    private final UUID b = UUID.randomUUID();

    @Test
    void hashIgnoresTheKeyButCoversEveryOtherField() {
        var base = new TransferCommand("k1", a, b, 100, "USD", "ref", Instant.parse("2030-01-01T00:00:00Z"));
        assertThat(new TransferCommand("k2", a, b, 100, "USD", "ref", base.occurredAt()).requestHash()).isEqualTo(base.requestHash());
        assertThat(new TransferCommand("k1", b, a, 100, "USD", "ref", base.occurredAt()).requestHash()).isNotEqualTo(base.requestHash());
        assertThat(new TransferCommand("k1", a, b, 101, "USD", "ref", base.occurredAt()).requestHash()).isNotEqualTo(base.requestHash());
        assertThat(new TransferCommand("k1", a, b, 100, "EUR", "ref", base.occurredAt()).requestHash()).isNotEqualTo(base.requestHash());
        assertThat(new TransferCommand("k1", a, b, 100, "USD", null, base.occurredAt()).requestHash()).isNotEqualTo(base.requestHash());
        assertThat(new TransferCommand("k1", a, b, 100, "USD", "ref", null).requestHash()).isNotEqualTo(base.requestHash());
    }
}
