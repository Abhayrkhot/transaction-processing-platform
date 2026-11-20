package io.github.abhayrkhot.ledger.account;

import io.github.abhayrkhot.ledger.common.LedgerException;
import io.github.abhayrkhot.ledger.common.LedgerProperties;
import java.util.List;
import java.util.UUID;
import org.springframework.stereotype.Service;

@Service
public class AccountService {

    private final AccountRepository accounts;
    private final AccountCache cache;
    private final int recentEntries;

    AccountService(AccountRepository accounts, AccountCache cache, LedgerProperties properties) {
        this.accounts = accounts;
        this.cache = cache;
        this.recentEntries = properties.recentEntries();
    }

    public Account open(String ownerId, Account.Type type, String currency) {
        return accounts.insert(ownerId, type, currency);
    }

    public AccountSummary summary(UUID id) {
        return cache.get(id).orElseGet(() -> {
            AccountSummary summary = accounts.summary(id, recentEntries)
                    .orElseThrow(() -> LedgerException.notFound("Account", id));
            cache.put(summary);
            return summary;
        });
    }

    public List<AccountSummary.Entry> entries(UUID id, Long beforeId, int limit) {
        if (accounts.findById(id).isEmpty()) {
            throw LedgerException.notFound("Account", id);
        }
        return accounts.entries(id, beforeId, limit);
    }
}
