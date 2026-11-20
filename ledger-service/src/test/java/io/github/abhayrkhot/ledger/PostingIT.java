package io.github.abhayrkhot.ledger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.abhayrkhot.ledger.account.Account;
import io.github.abhayrkhot.ledger.common.LedgerException;
import io.github.abhayrkhot.ledger.recon.ReconciliationService;
import io.github.abhayrkhot.ledger.transfer.PostingService;
import io.github.abhayrkhot.ledger.transfer.Transaction;
import io.github.abhayrkhot.ledger.transfer.TransferCommand;
import io.github.abhayrkhot.ledger.transfer.TransferResult;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

class PostingIT extends IntegrationTest {

    @Autowired
    ReconciliationService recon;

    @Test
    void postsBalancedEntriesAndMovesBalances() {
        Account a = customer(10_000);
        Account b = customer(0);

        TransferResult result = transfer(a.id(), b.id(), 2_500, "inv-1", null);

        assertThat(result.replayed()).isFalse();
        assertThat(result.transaction().status()).isEqualTo(Transaction.Status.POSTED);
        assertThat(balance(a.id())).isEqualTo(7_500);
        assertThat(balance(b.id())).isEqualTo(2_500);
        List<Long> legs = jdbc.sql("SELECT amount FROM entries WHERE transaction_id = :t ORDER BY amount")
                .param("t", result.transaction().id()).query(Long.class).list();
        assertThat(legs).containsExactly(-2_500L, 2_500L);
        assertThat(accounts.summary(b.id()).recentEntries().getFirst().balanceAfter()).isEqualTo(2_500);
    }

    @Test
    void insufficientFundsIsRecordedAsRejectedAndReplaysTheSameOutcome() {
        Account a = customer(1_000);
        Account b = customer(0);
        var cmd = new TransferCommand("k-" + UUID.randomUUID(), a.id(), b.id(), 5_000, "USD", null, null);

        TransferResult first = posting.transfer(cmd, PostingService.Channel.API);
        TransferResult again = posting.transfer(cmd, PostingService.Channel.API);

        assertThat(first.transaction().status()).isEqualTo(Transaction.Status.REJECTED);
        assertThat(first.transaction().rejectReason()).isEqualTo("INSUFFICIENT_FUNDS");
        assertThat(again.replayed()).isTrue();
        assertThat(again.transaction().id()).isEqualTo(first.transaction().id());
        assertThat(balance(a.id())).isEqualTo(1_000);
    }

    @Test
    void sameKeyWithDifferentPayloadIsRefused() {
        Account a = customer(10_000);
        Account b = customer(0);
        String key = "k-" + UUID.randomUUID();
        posting.transfer(new TransferCommand(key, a.id(), b.id(), 100, "USD", null, null), PostingService.Channel.API);

        assertThatThrownBy(() -> posting.transfer(new TransferCommand(key, a.id(), b.id(), 200, "USD", null, null),
                PostingService.Channel.API))
                .isInstanceOf(LedgerException.class)
                .hasMessageContaining("different request");
        assertThat(balance(a.id())).isEqualTo(9_900);
    }

    @Test
    void rejectsCurrencyMismatchAndUnknownAccounts() {
        Account usd = customer(10_000);
        Account eur = accounts.open("eur-owner", Account.Type.CUSTOMER, "EUR");

        assertThatThrownBy(() -> transfer(usd.id(), eur.id(), 100, null, null))
                .isInstanceOf(LedgerException.class).hasMessageContaining("currency");
        assertThatThrownBy(() -> transfer(usd.id(), UUID.randomUUID(), 100, null, null))
                .isInstanceOf(LedgerException.class).hasMessageContaining("not found");
    }

    @Test
    void concurrentDuplicatesOfOneKeyPostExactlyOnce() throws Exception {
        Account a = customer(10_000);
        Account b = customer(0);
        var cmd = new TransferCommand("k-" + UUID.randomUUID(), a.id(), b.id(), 700, "USD", null, null);

        List<TransferResult> results = runConcurrently(24, () -> posting.transfer(cmd, PostingService.Channel.KAFKA));

        assertThat(results).filteredOn(r -> !r.replayed()).hasSize(1);
        assertThat(results).extracting(r -> r.transaction().id()).containsOnly(results.getFirst().transaction().id());
        assertThat(balance(a.id())).isEqualTo(9_300);
        assertThat(balance(b.id())).isEqualTo(700);
    }

    /**
     * Many threads move money between a few accounts in both directions, with some
     * overdrafts and some retried keys. Afterwards money is conserved, nothing went negative,
     * every balance matches its entries, and each key posted at most once.
     */
    @Test
    void concurrentTransfersKeepTheLedgerConsistent() throws Exception {
        int accountsCount = 8;
        long funding = 50_000;
        List<UUID> ids = new ArrayList<>();
        for (int i = 0; i < accountsCount; i++) {
            ids.add(customer(funding).id());
        }
        List<String> usedKeys = Collections.synchronizedList(new ArrayList<>());
        Map<String, TransferCommand> commands = new ConcurrentHashMap<>();
        AtomicInteger errors = new AtomicInteger();

        runConcurrently(16, () -> {
            var random = ThreadLocalRandom.current();
            for (int i = 0; i < 250; i++) {
                TransferCommand cmd;
                if (!usedKeys.isEmpty() && random.nextInt(10) == 0) {
                    cmd = commands.get(usedKeys.get(random.nextInt(usedKeys.size())));
                } else {
                    UUID from = ids.get(random.nextInt(accountsCount));
                    UUID to = ids.get(random.nextInt(accountsCount));
                    if (from.equals(to)) {
                        continue;
                    }
                    cmd = new TransferCommand(UUID.randomUUID().toString(), from, to, 1 + random.nextInt(9_000), "USD", null, null);
                    commands.put(cmd.idempotencyKey(), cmd);
                    usedKeys.add(cmd.idempotencyKey());
                }
                try {
                    posting.transfer(cmd, PostingService.Channel.API);
                } catch (RuntimeException e) {
                    errors.incrementAndGet();
                }
            }
            return null;
        });

        assertThat(errors).hasValue(0);
        long total = ids.stream().mapToLong(this::balance).sum();
        assertThat(total).isEqualTo(accountsCount * funding);
        assertThat(ids).allSatisfy(id -> assertThat(balance(id)).isNotNegative());
        long transactions = jdbc.sql("SELECT count(*) FROM transactions WHERE idempotency_key IN (:keys)")
                .param("keys", List.copyOf(commands.keySet())).query(Long.class).single();
        assertThat(transactions).isEqualTo(commands.size());
        assertThat(recon.integrity().ok()).isTrue();
    }

    private static <T> List<T> runConcurrently(int threads, java.util.concurrent.Callable<T> task) throws Exception {
        try (ExecutorService pool = Executors.newFixedThreadPool(threads)) {
            List<Future<T>> futures = new ArrayList<>();
            for (int i = 0; i < threads; i++) {
                futures.add(pool.submit(task));
            }
            List<T> results = new ArrayList<>();
            for (Future<T> f : futures) {
                results.add(f.get());
            }
            return results;
        }
    }
}
