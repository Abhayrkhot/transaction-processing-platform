package io.github.abhayrkhot.ledger.transfer;

import io.github.abhayrkhot.ledger.account.Account;
import io.github.abhayrkhot.ledger.account.AccountCache;
import io.github.abhayrkhot.ledger.account.AccountRepository;
import io.github.abhayrkhot.ledger.common.LedgerException;
import io.github.abhayrkhot.ledger.common.LedgerProperties;
import io.github.abhayrkhot.ledger.outbox.OutboxRepository;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import tools.jackson.databind.json.JsonMapper;

/**
 * Posts transfers as double-entry transactions. One database transaction covers the
 * idempotency record, both entries, both balance updates and the outbox event, so a
 * transfer is either fully applied (and will be published) or not applied at all.
 */
@Service
public class PostingService {

    public enum Channel { API, KAFKA }

    static final String INSUFFICIENT_FUNDS = "INSUFFICIENT_FUNDS";

    private final AccountRepository accounts;
    private final TransactionRepository transactions;
    private final OutboxRepository outbox;
    private final AccountCache cache;
    private final JsonMapper json;
    private final MeterRegistry meters;
    private final String postedTopic;

    PostingService(AccountRepository accounts, TransactionRepository transactions, OutboxRepository outbox,
                   AccountCache cache, JsonMapper json, MeterRegistry meters, LedgerProperties properties) {
        this.accounts = accounts;
        this.transactions = transactions;
        this.outbox = outbox;
        this.cache = cache;
        this.json = json;
        this.meters = meters;
        this.postedTopic = properties.kafka().postedTopic();
    }

    @Transactional(timeout = 10)
    public TransferResult transfer(TransferCommand cmd, Channel channel) {
        if (cmd.fromAccountId().equals(cmd.toAccountId())) {
            throw LedgerException.invalid("SAME_ACCOUNT", "source and destination accounts must differ");
        }
        String hash = cmd.requestHash();

        // Fast path for retries: no row locks for a key we have already seen.
        var existing = transactions.findByIdempotencyKey(cmd.idempotencyKey());
        if (existing.isPresent()) {
            return replay(existing.get(), hash, channel);
        }

        List<Account> locked = accounts.lockForTransfer(cmd.fromAccountId(), cmd.toAccountId());
        Account from = find(locked, cmd.fromAccountId());
        Account to = find(locked, cmd.toAccountId());
        validate(cmd, from, to);

        boolean funded = from.isSystem() || from.balance() >= cmd.amount();
        Instant occurredAt = cmd.occurredAt() != null ? cmd.occurredAt() : Instant.now();
        var inserted = transactions.insert(cmd, hash, occurredAt,
                funded ? Transaction.Status.POSTED : Transaction.Status.REJECTED,
                funded ? null : INSUFFICIENT_FUNDS);
        if (inserted.isEmpty()) {
            // A concurrent request with this key committed while we waited for the locks.
            return replay(transactions.findByIdempotencyKey(cmd.idempotencyKey()).orElseThrow(), hash, channel);
        }
        Transaction txn = inserted.get();
        if (!funded) {
            count(channel, "rejected");
            return new TransferResult(txn, false);
        }

        transactions.insertEntries(txn.id(),
                from.id(), from.balance() - cmd.amount(),
                to.id(), to.balance() + cmd.amount(),
                cmd.amount());
        accounts.applyTransfer(from.id(), to.id(), cmd.amount());
        outbox.append(postedTopic, from.id().toString(),
                json.writeValueAsString(TransactionPostedEvent.of(txn, from, to)));
        invalidateCacheAfterCommit(from, to);
        count(channel, "posted");
        return new TransferResult(txn, false);
    }

    @Transactional(readOnly = true)
    public Transaction get(UUID id) {
        return transactions.findById(id).orElseThrow(() -> LedgerException.notFound("Transaction", id));
    }

    private TransferResult replay(Transaction txn, String hash, Channel channel) {
        if (!txn.requestHash().equals(hash)) {
            throw LedgerException.conflict("IDEMPOTENCY_KEY_REUSED",
                    "idempotency key " + txn.idempotencyKey() + " was already used with a different request");
        }
        count(channel, "replayed");
        return new TransferResult(txn, true);
    }

    private static Account find(List<Account> locked, UUID id) {
        return locked.stream()
                .filter(a -> a.id().equals(id))
                .findFirst()
                .orElseThrow(() -> LedgerException.notFound("Account", id));
    }

    private static void validate(TransferCommand cmd, Account from, Account to) {
        if (!from.currency().equals(cmd.currency()) || !to.currency().equals(cmd.currency())) {
            throw LedgerException.invalid("CURRENCY_MISMATCH",
                    "transfer currency " + cmd.currency() + " does not match both accounts");
        }
        if (from.status() != Account.Status.ACTIVE || to.status() != Account.Status.ACTIVE) {
            throw LedgerException.invalid("ACCOUNT_NOT_ACTIVE", "both accounts must be ACTIVE");
        }
    }

    private void invalidateCacheAfterCommit(Account from, Account to) {
        if (!cache.enabled()) {
            return;
        }
        long fromVersion = from.version() + 1;
        long toVersion = to.version() + 1;
        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            @Override
            public void afterCommit() {
                cache.invalidate(from.id(), fromVersion);
                cache.invalidate(to.id(), toVersion);
            }
        });
    }

    private void count(Channel channel, String outcome) {
        meters.counter("ledger.transfers", "channel", channel.name().toLowerCase(), "outcome", outcome).increment();
    }
}
