package io.github.abhayrkhot.ledger;

import io.github.abhayrkhot.ledger.account.Account;
import io.github.abhayrkhot.ledger.account.AccountService;
import io.github.abhayrkhot.ledger.transfer.PostingService;
import io.github.abhayrkhot.ledger.transfer.TransferCommand;
import io.github.abhayrkhot.ledger.transfer.TransferResult;
import java.time.Instant;
import java.util.UUID;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.simple.JdbcClient;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Import(TestcontainersConfig.class)
public abstract class IntegrationTest {

    @Autowired
    protected JdbcClient jdbc;

    @Autowired
    protected AccountService accounts;

    @Autowired
    protected PostingService posting;

    protected Account clearing() {
        return accounts.open("clearing-" + UUID.randomUUID(), Account.Type.SYSTEM, "USD");
    }

    /** A customer account funded from a fresh clearing account. */
    protected Account customer(long funding) {
        Account account = accounts.open("cust-" + UUID.randomUUID(), Account.Type.CUSTOMER, "USD");
        if (funding > 0) {
            transfer(clearing().id(), account.id(), funding, null, Instant.now().minusSeconds(86_400 * 60));
        }
        return account;
    }

    protected TransferResult transfer(UUID from, UUID to, long amount, String reference, Instant occurredAt) {
        return posting.transfer(new TransferCommand(UUID.randomUUID().toString(), from, to, amount, "USD", reference, occurredAt),
                PostingService.Channel.API);
    }

    protected long balance(UUID account) {
        return jdbc.sql("SELECT balance FROM accounts WHERE id = :id").param("id", account).query(Long.class).single();
    }
}
