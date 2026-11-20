package io.github.abhayrkhot.ledger.transfer;

import io.github.abhayrkhot.ledger.account.Account;
import java.time.Instant;
import java.util.UUID;

/** Published to the posted-transactions topic through the outbox. */
public record TransactionPostedEvent(
        UUID transactionId,
        UUID fromAccountId,
        Account.Type fromAccountType,
        UUID toAccountId,
        Account.Type toAccountType,
        long amount,
        String currency,
        String reference,
        Instant occurredAt,
        Instant postedAt) {

    static TransactionPostedEvent of(Transaction txn, Account from, Account to) {
        return new TransactionPostedEvent(txn.id(), from.id(), from.type(), to.id(), to.type(),
                txn.amount(), txn.currency(), txn.reference(), txn.occurredAt(), txn.postedAt());
    }
}
