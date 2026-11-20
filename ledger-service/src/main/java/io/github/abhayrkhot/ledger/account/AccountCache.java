package io.github.abhayrkhot.ledger.account;

import io.github.abhayrkhot.ledger.common.LedgerProperties;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Component;
import tools.jackson.databind.json.JsonMapper;

/**
 * Cache-aside for account summaries in Redis.
 *
 * <p>Plain delete-on-write has a race: a reader that loaded the old row before a posting
 * committed can write it back after the posting deleted the key. Every write here goes
 * through one Lua script that only accepts a newer score. Summaries are stored at score
 * 2*version; a posting that produced version v writes an empty tombstone at score 2*v-1,
 * which rejects any summary older than v and accepts the first reader that sees v.
 *
 * <p>Redis is an optimisation only. Any Redis error is treated as a miss, and the posting
 * path never reads from the cache.
 */
@Component
public class AccountCache {

    private static final Logger log = LoggerFactory.getLogger(AccountCache.class);

    private static final RedisScript<Long> PUT_IF_NEWER = RedisScript.of("""
            local current = redis.call('HGET', KEYS[1], 's')
            if current and tonumber(current) >= tonumber(ARGV[1]) then
              return 0
            end
            redis.call('HSET', KEYS[1], 's', ARGV[1], 'd', ARGV[2])
            redis.call('PEXPIRE', KEYS[1], ARGV[3])
            return 1
            """, Long.class);

    private final StringRedisTemplate redis;
    private final JsonMapper json;
    private final boolean enabled;
    private final String ttlMillis;
    private final Counter hits;
    private final Counter misses;
    private final Counter errors;

    AccountCache(StringRedisTemplate redis, JsonMapper json, LedgerProperties properties, MeterRegistry meters) {
        this.redis = redis;
        this.json = json;
        this.enabled = properties.accountCache().enabled();
        this.ttlMillis = Long.toString(properties.accountCache().ttl().toMillis());
        this.hits = meters.counter("ledger.account.cache", "result", "hit");
        this.misses = meters.counter("ledger.account.cache", "result", "miss");
        this.errors = meters.counter("ledger.account.cache", "result", "error");
    }

    public boolean enabled() {
        return enabled;
    }

    public Optional<AccountSummary> get(UUID accountId) {
        if (!enabled) {
            return Optional.empty();
        }
        try {
            Object data = redis.opsForHash().get(key(accountId), "d");
            if (data instanceof String s && !s.isEmpty()) {
                hits.increment();
                return Optional.of(json.readValue(s, AccountSummary.class));
            }
            misses.increment();
        } catch (RuntimeException e) {
            failed("read", e);
        }
        return Optional.empty();
    }

    public void put(AccountSummary summary) {
        if (enabled) {
            write(summary.id(), 2 * summary.version(), json.writeValueAsString(summary));
        }
    }

    /** Called after a posting commits; {@code newVersion} is the account version it produced. */
    public void invalidate(UUID accountId, long newVersion) {
        if (enabled) {
            write(accountId, 2 * newVersion - 1, "");
        }
    }

    private void write(UUID accountId, long score, String payload) {
        try {
            redis.execute(PUT_IF_NEWER, List.of(key(accountId)), Long.toString(score), payload, ttlMillis);
        } catch (RuntimeException e) {
            failed("write", e);
        }
    }

    private void failed(String op, RuntimeException e) {
        errors.increment();
        log.warn("account cache {} failed, falling back to Postgres: {}", op, e.toString());
    }

    private static String key(UUID accountId) {
        return "acct:" + accountId;
    }
}
