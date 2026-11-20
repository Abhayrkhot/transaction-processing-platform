package io.github.abhayrkhot.ledger;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.abhayrkhot.ledger.account.Account;
import io.github.abhayrkhot.ledger.account.AccountCache;
import io.github.abhayrkhot.ledger.account.AccountSummary;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

class AccountCacheIT extends IntegrationTest {

    @Autowired
    AccountCache cache;

    @Test
    void postingInvalidatesAndStaleWritesAreIgnored() {
        Account a = customer(10_000);
        Account b = customer(0);

        AccountSummary before = accounts.summary(a.id());
        assertThat(cache.get(a.id())).contains(before);

        transfer(a.id(), b.id(), 1_000, null, null);
        assertThat(cache.get(a.id())).isEmpty();

        // A reader that loaded the old row before the posting committed must not win.
        cache.put(before);
        assertThat(cache.get(a.id())).isEmpty();

        AccountSummary after = accounts.summary(a.id());
        assertThat(after.balance()).isEqualTo(9_000);
        assertThat(after.recentEntries().getFirst().amount()).isEqualTo(-1_000);
        assertThat(cache.get(a.id())).contains(after);
    }
}
